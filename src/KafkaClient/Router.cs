using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Assignment;
using KafkaClient.Common;
using KafkaClient.Connections;
using KafkaClient.Protocol;

namespace KafkaClient
{
    /// <summary>
    /// This class provides an abstraction from querying multiple Kafka servers for Metadata details and caching this data.
    ///
    /// All metadata queries are cached lazily.  If metadata from a topic does not exist in cache it will be queried for using
    /// the default brokers provided in the constructor.  Each Uri will be queried to get metadata information in turn until a
    /// response is received.  It is recommended therefore to provide more than one Kafka Uri as this API will be able to to get
    /// metadata information even if one of the Kafka servers goes down.
    ///
    /// The metadata will stay in cache until an error condition is received indicating the metadata is out of data.  This error
    /// can be in the form of a socket disconnect or an error code from a response indicating a broker no longer hosts a partition.
    /// </summary>
    public class Router : IRouter
    {
        private readonly IConnectionFactory _connectionFactory;
        private readonly Random _selector = new Random();

        private ImmutableDictionary<Endpoint, IImmutableList<IConnection>> _connections;
        private ImmutableDictionary<int, Endpoint> _brokerEndpoints = ImmutableDictionary<int, Endpoint>.Empty;
        private readonly SemaphoreSlim _connectionSemaphore = new SemaphoreSlim(1, 1);

        private ImmutableDictionary<string, Tuple<MetadataResponse.Topic, DateTimeOffset>> _topicCache = ImmutableDictionary<string, Tuple<MetadataResponse.Topic, DateTimeOffset>>.Empty;
        private readonly SemaphoreSlim _topicSemaphore = new SemaphoreSlim(1, 1);

        private ImmutableDictionary<string, Tuple<int, DateTimeOffset>> _groupBrokerCache = ImmutableDictionary<string, Tuple<int, DateTimeOffset>>.Empty;
        private readonly SemaphoreSlim _groupBrokerSemaphore = new SemaphoreSlim(1, 1);

        private ImmutableDictionary<string, Tuple<DescribeGroupsResponse.Group, DateTimeOffset>> _groupCache = ImmutableDictionary<string, Tuple<DescribeGroupsResponse.Group, DateTimeOffset>>.Empty;
        private readonly SemaphoreSlim _groupSemaphore = new SemaphoreSlim(1, 1);

        private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, IConnection>> _memberConnectionAssignment = new ConcurrentDictionary<string, ConcurrentDictionary<string, IConnection>>();
        private readonly ConcurrentDictionary<string, Tuple<IImmutableList<SyncGroupRequest.GroupAssignment>, int>> _memberAssignmentCache = new ConcurrentDictionary<string, Tuple<IImmutableList<SyncGroupRequest.GroupAssignment>, int>>();


        public static Task<Router> CreateAsync(
            Uri serverUri, IConnectionFactory connectionFactory = null,
            IConnectionConfiguration connectionConfiguration = null, 
            IRouterConfiguration routerConfiguration = null, ILog log = null)
        {
            return CreateAsync(new [] { serverUri }, connectionFactory, connectionConfiguration, routerConfiguration, log);
        }

        public static async Task<Router> CreateAsync(
            IEnumerable<Uri> serverUris, IConnectionFactory connectionFactory = null,
            IConnectionConfiguration connectionConfiguration = null,
            IRouterConfiguration routerConfiguration = null, ILog log = null)
        {
            var endpoints = new List<Endpoint>();
            log = log ?? TraceLog.Log;
            connectionFactory = connectionFactory ?? new ConnectionFactory();
            foreach (var uri in serverUris) {
                try {
                    endpoints.Add(await Endpoint.ResolveAsync(uri, log));
                } catch (ConnectionException ex) {
                    log.Warn(() => LogEvent.Create(ex, $"Ignoring uri that could not be resolved: {uri}"));
                }
            }
            return new Router(endpoints, connectionFactory, connectionConfiguration, routerConfiguration, log);
        }

        public Router(
            Endpoint endpoint, IConnectionFactory connectionFactory = null,
            IConnectionConfiguration connectionConfiguration = null, 
            IRouterConfiguration routerConfiguration = null, ILog log = null)
            : this (new []{ endpoint }, connectionFactory, connectionConfiguration, routerConfiguration, log)
        {
        }

        /// <exception cref="ConnectionException">None of the provided Kafka servers are resolvable.</exception>
        public Router(IEnumerable<Endpoint> endpoints, IConnectionFactory connectionFactory = null, IConnectionConfiguration connectionConfiguration = null, IRouterConfiguration routerConfiguration = null, ILog log = null)
        {
            Log = log ?? TraceLog.Log;
            ConnectionConfiguration = connectionConfiguration ?? new ConnectionConfiguration();
            _connectionFactory = connectionFactory ?? new ConnectionFactory();

            var connections = new Dictionary<Endpoint, IImmutableList<IConnection>>();
            foreach (var endpoint in endpoints) {
                try {
                    var connection = _connectionFactory.Create(endpoint, ConnectionConfiguration, Log);
                    connections[endpoint] = ImmutableList<IConnection>.Empty.Add(connection);
                } catch (ConnectionException ex) {
                    Log.Warn(() => LogEvent.Create(ex, $"Ignoring uri that could not be connected to: {endpoint}"));
                }
            }

            _connections = connections.ToImmutableDictionary();
            if (_connections.IsEmpty) throw new ConnectionException("None of the provided Kafka servers are resolvable.");

            Configuration = routerConfiguration ?? new RouterConfiguration();
        }

        public IConnectionConfiguration ConnectionConfiguration { get; }
        public IRouterConfiguration Configuration { get; }

        #region Topic Brokers

        /// <inheritdoc />
        public TopicBroker GetTopicBroker(string topicName, int partitionId)
        {
            return GetCachedTopicBroker(topicName, partitionId, GetCachedTopic(topicName));
        }

        private TopicBroker GetCachedTopicBroker(string topicName, int partitionId, MetadataResponse.Topic topic)
        {
            var partition = topic.Partitions.FirstOrDefault(x => x.PartitionId == partitionId);
            if (partition == null)
                throw new CachedMetadataException($"The topic ({topicName}) has no partitionId {partitionId} defined.") {
                    TopicName = topicName,
                    Partition = partitionId
                };

            return GetCachedTopicBroker(topicName, partition);
        }

        /// <inheritdoc />
        public async Task<TopicBroker> GetTopicBrokerAsync(string topicName, int partitionId, CancellationToken cancellationToken)
        {
            return GetCachedTopicBroker(topicName, partitionId, await GetTopicMetadataAsync(topicName, cancellationToken).ConfigureAwait(false));
        }

        private TopicBroker GetCachedTopicBroker(string topicName, MetadataResponse.Partition partition)
        {
            Endpoint endpoint;
            IImmutableList<IConnection> connections;
            if (_brokerEndpoints.TryGetValue(partition.LeaderId, out endpoint) && _connections.TryGetValue(endpoint, out connections)) {
                var index = _selector.Next(0, connections.Count - 1);
                return new TopicBroker(topicName, partition.PartitionId, partition.LeaderId, connections[index]);
            }

            throw new CachedMetadataException($"Lead broker cannot be found for partition/{partition.PartitionId}, leader {partition.LeaderId}") {
                TopicName = topicName,
                Partition = partition.PartitionId
            };
        }

        #endregion

        #region Topic Metadata

        /// <inheritdoc />
        public MetadataResponse.Topic GetTopicMetadata(string topicName)
        {
            return GetCachedTopic(topicName);
        }

        /// <inheritdoc />
        public IImmutableList<MetadataResponse.Topic> GetTopicMetadata(IEnumerable<string> topicNames)
        {
            var cachedResults = CachedResults<MetadataResponse.Topic>.ProduceResults(topicNames, topicName => TryGetCachedTopic(topicName));
            if (cachedResults.Misses.Count > 0) throw new CachedMetadataException($"No metadata defined for topics: {string.Join(",", cachedResults.Misses)}");

            return ImmutableList<MetadataResponse.Topic>.Empty.AddRange(cachedResults.Hits);
        }

        /// <inheritdoc />
        public IImmutableList<MetadataResponse.Topic> GetTopicMetadata()
        {
            return ImmutableList<MetadataResponse.Topic>.Empty.AddRange(_topicCache.Values.Select(t => t.Item1));
        }

        /// <inheritdoc />
        public async Task<MetadataResponse.Topic> GetTopicMetadataAsync(string topicName, CancellationToken cancellationToken)
        {
            return TryGetCachedTopic(topicName) 
                ?? await UpdateTopicMetadataFromServerAsync(topicName, false, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task<IImmutableList<MetadataResponse.Topic>> GetTopicMetadataAsync(IEnumerable<string> topicNames, CancellationToken cancellationToken)
        {
            var cachedResults = CachedResults<MetadataResponse.Topic>.ProduceResults(topicNames, topicName => TryGetCachedTopic(topicName));
            return cachedResults.Misses.Count == 0 
                ? cachedResults.Hits 
                : cachedResults.Hits.AddRange(await UpdateTopicMetadataFromServerAsync(cachedResults.Misses, false, cancellationToken).ConfigureAwait(false));
        }

        /// <inheritdoc />
        public Task RefreshTopicMetadataAsync(string topicName, bool ignoreCacheExpiry, CancellationToken cancellationToken)
        {
            return UpdateTopicMetadataFromServerAsync(topicName, ignoreCacheExpiry, cancellationToken);
        }

        /// <inheritdoc />
        public Task RefreshTopicMetadataAsync(IEnumerable<string> topicNames, bool ignoreCacheExpiry, CancellationToken cancellationToken)
        {
            return UpdateTopicMetadataFromServerAsync(topicNames, ignoreCacheExpiry, cancellationToken);
        }

        /// <inheritdoc />
        public Task RefreshTopicMetadataAsync(CancellationToken cancellationToken)
        {
            return UpdateTopicMetadataFromServerAsync((IEnumerable<string>) null, true, cancellationToken);
        }

        private MetadataResponse.Topic GetCachedTopic(string topicName, TimeSpan? expiration = null)
        {
            var topic = TryGetCachedTopic(topicName, expiration);
            if (topic != null) return topic;

            throw new CachedMetadataException($"No metadata defined for topic/{topicName}") { TopicName = topicName };
        }

        private MetadataResponse.Topic TryGetCachedTopic(string topicName, TimeSpan? expiration = null)
        {
            Tuple<MetadataResponse.Topic, DateTimeOffset> cachedValue;
            if (_topicCache.TryGetValue(topicName, out cachedValue) && !HasExpired(cachedValue, expiration)) {
                return cachedValue.Item1;
            }
            return null;
        }

        private async Task<MetadataResponse.Topic> UpdateTopicMetadataFromServerAsync(string topicName, bool ignoreCache, CancellationToken cancellationToken)
        {
            var topics = await UpdateTopicMetadataFromServerAsync(new [] { topicName }, ignoreCache, cancellationToken).ConfigureAwait(false);
            return topics.SingleOrDefault();
        }

        private async Task<IImmutableList<MetadataResponse.Topic>> UpdateTopicMetadataFromServerAsync(IEnumerable<string> topicNames, bool ignoreCache, CancellationToken cancellationToken)
        {
            return await _topicSemaphore.LockAsync(
                async () => {
                    var cachedResults = new CachedResults<MetadataResponse.Topic>(misses: topicNames);
                    if (!ignoreCache) {
                        cachedResults = CachedResults<MetadataResponse.Topic>.ProduceResults(cachedResults.Misses, topicName => TryGetCachedTopic(topicName, Configuration.CacheExpiration));
                        if (cachedResults.Misses.Count == 0) return cachedResults.Hits;
                    }

                    MetadataRequest request;
                    MetadataResponse response;
                    if (ignoreCache && topicNames == null) {
                        Log.Info(() => LogEvent.Create("Router refreshing metadata for all topics"));
                        request = new MetadataRequest();
                        response = await this.GetMetadataAsync(request, cancellationToken).ConfigureAwait(false);
                    } else {
                        Log.Info(() => LogEvent.Create($"Router refreshing metadata for topics {string.Join(",", cachedResults.Misses)}"));
                        request = new MetadataRequest(cachedResults.Misses);
                        response = await this.GetMetadataAsync(request, cancellationToken).ConfigureAwait(false);
                    }

                    if (response != null) {
                        await UpdateConnectionCacheAsync(response.Brokers, cancellationToken);
                    }
                    UpdateTopicCache(response);

                    // since the above may take some time to complete, it's necessary to hold on to the topics we found before
                    // just in case they expired between when we searched for them and now.
                    var result = cachedResults.Hits.AddNotNullRange(response?.Topics);
                    return result;
                }, cancellationToken).ConfigureAwait(false);
        }

        private CachedMetadataException GetPartitionElectionException(IList<TopicPartition> partitionElections)
        {
            var topic = partitionElections.FirstOrDefault();
            if (topic == null) return null;

            var message = $"Leader Election for topic {topic.TopicName} partition {topic.PartitionId}";
            var innerException = GetPartitionElectionException(partitionElections.Skip(1).ToList());
            var exception = innerException != null
                                ? new CachedMetadataException(message, innerException)
                                : new CachedMetadataException(message);
            exception.TopicName = topic.TopicName;
            exception.Partition = topic.PartitionId;
            return exception;
        }

        private void UpdateTopicCache(MetadataResponse metadata)
        {
            if (metadata == null) return;

            var partitionElections = metadata.Topics.SelectMany(
                t => t.Partitions
                      .Where(p => p.IsElectingLeader)
                      .Select(p => new TopicPartition(t.TopicName, p.PartitionId)))
                      .ToList();
            if (partitionElections.Any()) throw GetPartitionElectionException(partitionElections);

            var topicCache = _topicCache;
            try {
                foreach (var topic in metadata.Topics) {
                    topicCache = topicCache.SetItem(topic.TopicName, new Tuple<MetadataResponse.Topic, DateTimeOffset>(topic, DateTimeOffset.UtcNow));
                }
            } finally {
                _topicCache = topicCache;
            }
        }

        #endregion

        #region Group Brokers

        /// <inheritdoc />
        public GroupBroker GetGroupBroker(string groupId)
        {
            return GetCachedGroupBroker(groupId, GetCachedGroupBrokerId(groupId));
        }

        /// <inheritdoc />
        public async Task<GroupBroker> GetGroupBrokerAsync(string groupId, CancellationToken cancellationToken)
        {
            return GetCachedGroupBroker(groupId, await GetGroupBrokerIdAsync(groupId, cancellationToken).ConfigureAwait(false));
        }

        /// <inheritdoc />
        public async Task<int> GetGroupBrokerIdAsync(string groupId, CancellationToken cancellationToken)
        {
            return TryGetCachedGroupBrokerId(groupId) 
                ?? await UpdateGroupBrokersFromServerAsync(groupId, false, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public Task RefreshGroupBrokerAsync(string groupId, bool ignoreCacheExpiry, CancellationToken cancellationToken)
        {
            return UpdateGroupBrokersFromServerAsync(groupId, ignoreCacheExpiry, cancellationToken);
        }

        private GroupBroker GetCachedGroupBroker(string groupId, int brokerId)
        {
            Endpoint endpoint;
            IImmutableList<IConnection> connections;
            if (_brokerEndpoints.TryGetValue(brokerId, out endpoint) && _connections.TryGetValue(endpoint, out connections)) {
                var index = _selector.Next(0, connections.Count - 1);
                return new GroupBroker(groupId, brokerId, connections[index]);
            }

            throw new CachedMetadataException($"Broker cannot be found for group/{groupId}, broker {brokerId}");
        }

        private int GetCachedGroupBrokerId(string groupId, TimeSpan? expiration = null)
        {
            var brokerId = TryGetCachedGroupBrokerId(groupId, expiration);
            if (brokerId.HasValue) return brokerId.Value;

            throw new CachedMetadataException($"No metadata defined for group/{groupId}");
        }

        private int? TryGetCachedGroupBrokerId(string groupId, TimeSpan? expiration = null)
        {
            Tuple<int, DateTimeOffset> cachedValue;
            if (_groupBrokerCache.TryGetValue(groupId, out cachedValue) && !HasExpired(cachedValue, expiration)) {
                return cachedValue.Item1;
            }
            return null;
        }

        private async Task<int> UpdateGroupBrokersFromServerAsync(string groupId, bool ignoreCache, CancellationToken cancellationToken)
        {
            return await _groupBrokerSemaphore.LockAsync(
                async () => {
                    if (!ignoreCache) {
                        var brokerId = TryGetCachedGroupBrokerId(groupId, Configuration.CacheExpiration);
                        if (brokerId.HasValue) return brokerId.Value;
                    }

                    Log.Info(() => LogEvent.Create($"Router refreshing brokers for group {groupId}"));
                    var request = new GroupCoordinatorRequest(groupId);
                    try {
                        var response = await this.SendToAnyAsync(request, cancellationToken).ConfigureAwait(false);

                        if (response != null) {
                            await UpdateConnectionCacheAsync(new [] { response }, cancellationToken);
                        }
                        UpdateGroupBrokerCache(request, response);

                        // ReSharper disable once PossibleNullReferenceException
                        return response.BrokerId;
                    } catch (Exception ex) {
                        throw new CachedMetadataException($"Unable to refresh brokers for group {groupId}", ex);
                    }
                }, cancellationToken).ConfigureAwait(false);
        }

        private void UpdateGroupBrokerCache(GroupCoordinatorRequest request, GroupCoordinatorResponse response)
        {
            if (request == null || response == null) return;

            _groupBrokerCache = _groupBrokerCache.SetItem(request.GroupId, new Tuple<int, DateTimeOffset>(response.BrokerId, DateTimeOffset.UtcNow));
        }

        #endregion

        #region Group Metadata

        /// <inheritdoc />
        public DescribeGroupsResponse.Group GetGroupMetadata(string groupId)
        {
            return GetCachedGroup(groupId);
        }

        /// <inheritdoc />
        public IImmutableList<DescribeGroupsResponse.Group> GetGroupMetadata(IEnumerable<string> groupIds)
        {
            var cachedResults = CachedResults<DescribeGroupsResponse.Group>.ProduceResults(groupIds, groupId => TryGetCachedGroup(groupId));
            if (cachedResults.Misses.Count > 0) throw new CachedMetadataException($"No metadata defined for groups: {string.Join(",", cachedResults.Misses)}");

            return ImmutableList<DescribeGroupsResponse.Group>.Empty.AddRange(cachedResults.Hits);
        }

        /// <inheritdoc />
        public async Task<DescribeGroupsResponse.Group> GetGroupMetadataAsync(string groupId, CancellationToken cancellationToken)
        {
            return TryGetCachedGroup(groupId) 
                ?? await UpdateGroupMetadataFromServerAsync(groupId, false, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task<IImmutableList<DescribeGroupsResponse.Group>> GetGroupMetadataAsync(IEnumerable<string> groupIds, CancellationToken cancellationToken)
        {
            var cachedResults = CachedResults<DescribeGroupsResponse.Group>.ProduceResults(groupIds, groupId => TryGetCachedGroup(groupId));
            return cachedResults.Misses.Count == 0 
                ? cachedResults.Hits 
                : cachedResults.Hits.AddRange(await UpdateGroupMetadataFromServerAsync(cachedResults.Misses, false, cancellationToken).ConfigureAwait(false));
        }

        /// <inheritdoc />
        public Task RefreshGroupMetadataAsync(string groupId, bool ignoreCacheExpiry, CancellationToken cancellationToken)
        {
            return UpdateGroupMetadataFromServerAsync(groupId, ignoreCacheExpiry, cancellationToken);
        }

        /// <inheritdoc />
        public Task RefreshGroupMetadataAsync(IEnumerable<string> groupIds, bool ignoreCacheExpiry, CancellationToken cancellationToken)
        {
            return UpdateGroupMetadataFromServerAsync(groupIds, ignoreCacheExpiry, cancellationToken);
        }

        private DescribeGroupsResponse.Group GetCachedGroup(string groupId, TimeSpan? expiration = null)
        {
            var group = TryGetCachedGroup(groupId, expiration);
            if (group != null) return group;

            throw new CachedMetadataException($"No metadata defined for group/{groupId}");
        }

        private DescribeGroupsResponse.Group TryGetCachedGroup(string groupId, TimeSpan? expiration = null)
        {
            Tuple<DescribeGroupsResponse.Group, DateTimeOffset> cachedValue;
            if (_groupCache.TryGetValue(groupId, out cachedValue) && !HasExpired(cachedValue, expiration)) {
                return cachedValue.Item1;
            }
            return null;
        }

        private async Task<DescribeGroupsResponse.Group> UpdateGroupMetadataFromServerAsync(string groupId, bool ignoreCache, CancellationToken cancellationToken)
        {
            var groups = await UpdateGroupMetadataFromServerAsync(new [] { groupId }, ignoreCache, cancellationToken).ConfigureAwait(false);
            return groups.SingleOrDefault();
        }
        
        private async Task<IImmutableList<DescribeGroupsResponse.Group>> UpdateGroupMetadataFromServerAsync(IEnumerable<string> groupIds, bool ignoreCache, CancellationToken cancellationToken)
        {
            return await _groupSemaphore.LockAsync(
                async () => {
                    var cachedResults = new CachedResults<DescribeGroupsResponse.Group>(misses: groupIds);
                    if (!ignoreCache) {
                        cachedResults = CachedResults<DescribeGroupsResponse.Group>.ProduceResults(cachedResults.Misses, groupId => TryGetCachedGroup(groupId, Configuration.CacheExpiration));
                        if (cachedResults.Misses.Count == 0) return cachedResults.Hits;
                    }

                    Log.Info(() => LogEvent.Create($"Router refreshing metadata for groups {string.Join(",", cachedResults.Misses)}"));
                    var request = new DescribeGroupsRequest(cachedResults.Misses);
                    var response = await this.SendToAnyAsync(request, cancellationToken).ConfigureAwait(false);

                    UpdateGroupCache(response);

                    // since the above may take some time to complete, it's necessary to hold on to the groups we found before
                    // just in case they expired between when we searched for them and now.
                    var result = cachedResults.Hits.AddNotNullRange(response?.Groups);
                    return result;
                }, cancellationToken).ConfigureAwait(false);
        }

        private void UpdateGroupCache(DescribeGroupsResponse metadata)
        {
            if (metadata == null) return;

            var groupCache = _groupCache;
            try {
                foreach (var group in metadata.Groups) {
                    groupCache = groupCache.SetItem(group.GroupId, new Tuple<DescribeGroupsResponse.Group, DateTimeOffset>(group, DateTimeOffset.UtcNow));
                }
            } finally {
                _groupCache = groupCache;
            }
        }

        #endregion

        #region Member Assignments

        public Task<SyncGroupResponse> SyncGroupAsync(SyncGroupRequest request, IRequestContext context, IRetry retryPolicy, CancellationToken cancellationToken)
        {
            if (request.GroupAssignments.Count > 0) {
                var value = new Tuple<IImmutableList<SyncGroupRequest.GroupAssignment>, int>(request.GroupAssignments, request.GenerationId);
                _memberAssignmentCache.AddOrUpdate(request.GroupId, value, (key, old) => value);
            }

            return this.SendAsync(request, request.GroupId, cancellationToken, context, retryPolicy); 
        }

        public IImmutableDictionary<string, IMemberAssignment> GetGroupMemberAssignment(string groupId, int? generationId = null)
        {
            var assignment = TryGetCachedMemberAssignment(groupId, generationId);
            if (assignment == null && !generationId.HasValue) {
                assignment = TryGetCachedGroup(groupId)?.Members?.ToImmutableDictionary(member => member.MemberId, member => member.MemberAssignment);
            }
            return assignment ?? ImmutableDictionary<string, IMemberAssignment>.Empty;
        }

        private IImmutableDictionary<string, IMemberAssignment> TryGetCachedMemberAssignment(string groupId, int? generationId = null)
        {
            Tuple<IImmutableList<SyncGroupRequest.GroupAssignment>, int> cachedValue;
            if (_memberAssignmentCache.TryGetValue(groupId, out cachedValue) && (!generationId.HasValue || generationId.Value == cachedValue.Item2)) {
                return cachedValue.Item1.ToImmutableDictionary(assignment => assignment.MemberId, assignment => assignment.MemberAssignment);
            }
            return null;
        }

        #endregion

        private static bool HasExpired<T>(Tuple<T, DateTimeOffset> cachedValue, TimeSpan? expiration = null)
        {
            return expiration.HasValue && expiration.Value < DateTimeOffset.UtcNow - cachedValue.Item2;
        }

        private class CachedResults<T>
        {
            public IImmutableList<T> Hits { get; }
            public IImmutableList<string> Misses { get; }

            public CachedResults(IEnumerable<T> hits = null, IEnumerable<string> misses = null)
            {
                Hits = ImmutableList<T>.Empty.AddNotNullRange(hits);
                Misses = ImmutableList<string>.Empty.AddNotNullRange(misses);
            }

            public static CachedResults<T> ProduceResults(IEnumerable<string> keys, Func<string, T> producer)
            {
                var misses = new List<string>();
                var hits = new List<T>();

                foreach (var key in keys.Distinct()) {
                    var value = producer(key);
                    if (value != null) {
                        hits.Add(value);
                    } else {
                        misses.Add(key);
                    }
                }

                return new CachedResults<T>(hits, misses);
            }
        }

        #region Connections

        /// <inheritdoc />
        public IEnumerable<IConnection> Connections => _connections.Values.Select(connections => connections[0]);

        /// <inheritdoc />
        public async Task<IConnection> GetConnectionAsync(string groupId, string memberId, CancellationToken cancellationToken)
        {
            var memberConnections = _memberConnectionAssignment.GetOrAdd(groupId, key => new ConcurrentDictionary<string, IConnection>());
            IConnection connection;
            // check if already assigned
            if (memberConnections.TryGetValue(memberId, out connection)) return connection;

            var brokerId = await GetGroupBrokerIdAsync(groupId, cancellationToken);
            Endpoint endpoint;
            if (!_brokerEndpoints.TryGetValue(brokerId, out endpoint)) {
                throw new CachedMetadataException($"Expected to resolve endpoint for broker id {brokerId}");
            }

            return _connectionSemaphore.Lock(() => {
                // try again to avoid race conditions while waiting on lock
                if (memberConnections.TryGetValue(memberId, out connection)) return connection;

                IImmutableList<IConnection> connections;
                if (!_connections.TryGetValue(endpoint, out connections)) {
                    connections = ImmutableList<IConnection>.Empty;
                }
                var assignedConnections = memberConnections.Values.ToList();
                connection = connections.Except(assignedConnections).FirstOrDefault();
                if (connection == null) {
                    connection = _connectionFactory.Create(endpoint, ConnectionConfiguration, Log);
                    _connections = _connections.SetItem(endpoint, connections.Add(connection));
                }

                memberConnections[memberId] = connection;
                return connection;
            }, cancellationToken);
        }

        /// <inheritdoc />
        public void ReturnConnection(string groupId, string memberId, IConnection connection)
        {
            ConcurrentDictionary<string, IConnection> memberConnections;
            if (!_memberConnectionAssignment.TryGetValue(groupId, out memberConnections)) {
                Log.Warn(() => LogEvent.Create($"Router could not find connections assigned to {groupId}"));
                return;
            }
            IConnection removed;
            if (!memberConnections.TryRemove(memberId, out removed)) {
                Log.Warn(() => LogEvent.Create($"Router could not find and remove connection assigned to {groupId} {memberId}"));
            } else if (!ReferenceEquals(connection, removed)) {
                Log.Warn(() => LogEvent.Create($"Router remove different connection than assigned to {{GroupId:{groupId},MemberId:{memberId}}}"));
            }
        }

        public bool TryRestore(IConnection connection, CancellationToken cancellationToken)
        {
            if (_disposeCount > 0) throw new ObjectDisposedException(nameof(Router));
            if (connection == null) return false;

            var endpoint = connection.Endpoint;
            IImmutableList<IConnection> ownedConnections;
            // false if the endpoint isn't part of the router
            // true if the one in the router is already restore (or will by itself)
            if (!_connections.TryGetValue(endpoint, out ownedConnections)) return false;
            var ownedConnection = ownedConnections.SingleOrDefault(owned => ReferenceEquals(owned, connection));
            if (ownedConnection == null) return false;
            if (!ownedConnection.IsDisposed) return true;

            // actually restore the connection
            return _connectionSemaphore.Lock(
                () => {
                    // test again (same logic as above) -- to avoid race conditions
                    if (!_connections.TryGetValue(endpoint, out ownedConnections)) return false;
                    ownedConnection = ownedConnections.SingleOrDefault(owned => ReferenceEquals(owned, connection));
                    if (ownedConnection == null) return false;

                    ownedConnections = ownedConnections.Replace(ownedConnection, _connectionFactory.Create(endpoint, ConnectionConfiguration, Log));
                    _connections = _connections.SetItem(endpoint, ownedConnections);
                    return true;
                }, cancellationToken);
        }

        private async Task UpdateConnectionCacheAsync(IEnumerable<Protocol.Broker> brokers, CancellationToken cancellationToken)
        {
            await _connectionSemaphore.LockAsync(async () => {
                var connections = _connections;
                var brokerEndpoints = _brokerEndpoints;
                try {
                    var hasNewBrokers = false;
                    foreach (var server in brokers) {
                        Endpoint existing;
                        if (brokerEndpoints.TryGetValue(server.BrokerId, out existing) 
                            && existing.Host == server.Host 
                            && existing.Ip.Port == server.Port)
                        {
                            continue; // same as we already have
                        }

                        var endpoint = await Endpoint.ResolveAsync(new Uri($"http://{server.Host}:{server.Port}"), Log);
                        brokerEndpoints = brokerEndpoints.SetItem(server.BrokerId, endpoint);
                        hasNewBrokers = true;
                    }

                    if (!hasNewBrokers) return;

                    // only keep if they're alive
                    connections = connections.SelectMany(pair => pair.Value.Where(connection => !connection.IsDisposed))
                                             .GroupBy(connection => connection.Endpoint)
                                             .ToImmutableDictionary(group => group.Key, group => (IImmutableList<IConnection>)group.ToImmutableList());
                    foreach (var endpoint in brokerEndpoints.Values) {
                        if (!connections.ContainsKey(endpoint)) {
                            connections = connections.SetItem(endpoint, ImmutableList<IConnection>.Empty.Add(_connectionFactory.Create(endpoint, ConnectionConfiguration, Log)));
                        }
                    }
                } finally {
                    _connections = connections.ToImmutableDictionary(pair => pair.Key, pair => (IImmutableList<IConnection>)pair.Value.ToImmutableList());
                    _brokerEndpoints = brokerEndpoints;
                }
            }, cancellationToken);
        }

        private async Task DisposeConnectionsAsync(IEnumerable<IConnection> connections)
        {
            await Task.WhenAll(connections.Select(_ => _.DisposeAsync()));
        }

        #endregion

        private int _disposeCount = 0;
        private readonly TaskCompletionSource<bool> _disposePromise = new TaskCompletionSource<bool>();

        public async Task DisposeAsync()
        {
            if (Interlocked.Increment(ref _disposeCount) != 1) {
                await _disposePromise.Task;
                return;
            }

            try {
                Log.Debug(() => LogEvent.Create("Disposing Router"));
                await DisposeConnectionsAsync(_connections.SelectMany(pair => pair.Value));
                _connectionSemaphore.Dispose();
                _groupSemaphore.Dispose();
                _groupBrokerSemaphore.Dispose();
                _topicSemaphore.Dispose();
            } finally {
                _disposePromise.TrySetResult(true);
            }
        }

        /// <inheritdoc />
        public void Dispose()
        {
#pragma warning disable 4014
            // trigger, and set the promise appropriately
            DisposeAsync();
#pragma warning restore 4014
        }

        /// <inheritdoc />
        public ILog Log { get; }
    }
}