using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Connection;
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
    public class BrokerRouter : IBrokerRouter
    {
        private readonly IConnectionFactory _connectionFactory;
        private readonly IPartitionSelector _partitionSelector;

        private ImmutableDictionary<Endpoint, IConnection> _allConnections = ImmutableDictionary<Endpoint, IConnection>.Empty;
        private ImmutableDictionary<int, IConnection> _brokerConnections = ImmutableDictionary<int, IConnection>.Empty;
        private ImmutableDictionary<string, Tuple<MetadataTopic, DateTime>> _topicCache = ImmutableDictionary<string, Tuple<MetadataTopic, DateTime>>.Empty;

        private readonly AsyncLock _lock = new AsyncLock();

        /// <exception cref="ConnectionException">None of the provided Kafka servers are resolvable.</exception>
        public BrokerRouter(KafkaOptions options)
            : this(options.ServerUris, options.ConnectionFactory, options.ConnectionConfiguration, options.PartitionSelector, options.CacheConfiguration, options.Log)
        {
        }

        /// <exception cref="ConnectionException">None of the provided Kafka servers are resolvable.</exception>
        public BrokerRouter(Uri serverUri, IConnectionFactory connectionFactory = null, IConnectionConfiguration connectionConfiguration = null, IPartitionSelector partitionSelector = null, ICacheConfiguration cacheConfiguration = null, ILog log = null)
            : this (new []{ serverUri }, connectionFactory, connectionConfiguration, partitionSelector, cacheConfiguration, log)
        {
        }

        /// <exception cref="ConnectionException">None of the provided Kafka servers are resolvable.</exception>
        public BrokerRouter(IEnumerable<Uri> serverUris, IConnectionFactory connectionFactory = null, IConnectionConfiguration connectionConfiguration = null, IPartitionSelector partitionSelector = null, ICacheConfiguration cacheConfiguration = null, ILog log = null)
        {
            Log = log ?? TraceLog.Log;
            Configuration = connectionConfiguration ?? new ConnectionConfiguration();
            _connectionFactory = connectionFactory ?? new ConnectionFactory();

            foreach (var uri in serverUris) {
                try {
                    var endpoint = _connectionFactory.Resolve(uri, Log);
                    var connection = _connectionFactory.Create(endpoint, Configuration, Log);
                    _allConnections = _allConnections.SetItem(endpoint, connection);
                } catch (ConnectionException ex) {
                    Log.WarnFormat(ex, "Ignoring uri that could not be resolved: {0}", uri);
                }
            }

            if (_allConnections.IsEmpty) throw new ConnectionException("None of the provided Kafka servers are resolvable.");

            CacheConfiguration = cacheConfiguration ?? new CacheConfiguration();
            _partitionSelector = partitionSelector ?? new PartitionSelector();
        }

        public IConnectionConfiguration Configuration { get; }
        public ICacheConfiguration CacheConfiguration { get; }

        /// <inheritdoc />
        public BrokerRoute GetBrokerRoute(string topicName, int partitionId)
        {
            return GetBrokerRoute(topicName, partitionId, GetCachedTopic(topicName));
        }

        private BrokerRoute GetBrokerRoute(string topicName, int partitionId, MetadataTopic topic)
        {
            var partition = topic.Partitions.FirstOrDefault(x => x.PartitionId == partitionId);
            if (partition == null)
                throw new CachedMetadataException($"The topic ({topicName}) has no partitionId {partitionId} defined.") {
                    Topic = topicName,
                    Partition = partitionId
                };

            return GetCachedRoute(topicName, partition);
        }

        /// <inheritdoc />
        public BrokerRoute GetBrokerRoute(string topicName, byte[] key = null)
        {
            var topic = GetCachedTopic(topicName);
            return GetCachedRoute(topicName, _partitionSelector.Select(topic, key));
        }

        /// <inheritdoc />
        public async Task<BrokerRoute> GetBrokerRouteAsync(string topicName, int partitionId, CancellationToken cancellationToken)
        {
            return GetBrokerRoute(topicName, partitionId, await GetTopicMetadataAsync(topicName, cancellationToken));
        }

        /// <inheritdoc />
        public MetadataTopic GetTopicMetadata(string topicName)
        {
            return GetCachedTopic(topicName);
        }

        /// <inheritdoc />
        public ImmutableList<MetadataTopic> GetTopicMetadata(IEnumerable<string> topicNames)
        {
            var topicSearchResult = TryGetCachedTopics(topicNames);
            if (topicSearchResult.Missing.Count > 0) throw new CachedMetadataException($"No metadata defined for topics: {string.Join(",", topicSearchResult.Missing)}");

            return ImmutableList<MetadataTopic>.Empty.AddRange(topicSearchResult.Topics);
        }

        /// <inheritdoc />
        public ImmutableList<MetadataTopic> GetTopicMetadata()
        {
            return ImmutableList<MetadataTopic>.Empty.AddRange(_topicCache.Values.Select(t => t.Item1));
        }

        /// <inheritdoc />
        public async Task<MetadataTopic> GetTopicMetadataAsync(string topicName, CancellationToken cancellationToken)
        {
            return TryGetCachedTopic(topicName) 
                ?? await UpdateTopicMetadataFromServerIfMissingAsync(topicName, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task<ImmutableList<MetadataTopic>> GetTopicMetadataAsync(IEnumerable<string> topicNames, CancellationToken cancellationToken)
        {
            var searchResult = TryGetCachedTopics(topicNames);
            return searchResult.Missing.IsEmpty 
                ? searchResult.Topics 
                : searchResult.Topics.AddRange(await UpdateTopicMetadataFromServerIfMissingAsync(searchResult.Missing, cancellationToken).ConfigureAwait(false));
        }

        /// <inheritdoc />
        public Task RefreshTopicMetadataAsync(string topicName, bool ignoreCacheExpiry, CancellationToken cancellationToken)
        {
            return ignoreCacheExpiry 
                ? UpdateTopicMetadataFromServerAsync(topicName, cancellationToken)
                : UpdateTopicMetadataFromServerIfMissingAsync(topicName, cancellationToken);
        }

        /// <inheritdoc />
        public Task RefreshTopicMetadataAsync(CancellationToken cancellationToken)
        {
            return UpdateTopicMetadataFromServerAsync(null, cancellationToken);
        }

        private async Task<MetadataTopic> UpdateTopicMetadataFromServerIfMissingAsync(string topicName, CancellationToken cancellationToken)
        {
            var topics = await UpdateTopicMetadataFromServerIfMissingAsync(new [] { topicName }, cancellationToken).ConfigureAwait(false);
            return topics.Single();
        }

        private async Task<ImmutableList<MetadataTopic>> UpdateTopicMetadataFromServerIfMissingAsync(IEnumerable<string> topicNames, CancellationToken cancellationToken)
        {
            // TODO: more sophisticated locking should be particular to topicName(s) in that multiple 
            // requests can be made in parallel for different topicName(s).
            using (await _lock.LockAsync(cancellationToken).ConfigureAwait(false)) {
                var searchResult = TryGetCachedTopics(topicNames, CacheConfiguration.CacheExpiration);
                if (searchResult.Missing.Count == 0) return searchResult.Topics;

                Log.DebugFormat("BrokerRouter refreshing metadata for topics {0}", string.Join(",", searchResult.Missing));
                var response = await GetTopicMetadataFromServerAsync(searchResult.Missing, cancellationToken);
                UpdateConnectionCache(response);
                UpdateTopicCache(response);

                // since the above may take some time to complete, it's necessary to hold on to the topics we found before
                // just in case they expired between when we searched for them and now.
                return response.Topics.AddRange(searchResult.Topics);
            }
        }

        private async Task UpdateTopicMetadataFromServerAsync(string topicName, CancellationToken cancellationToken)
        {
            // TODO: more sophisticated locking should be particular to topicName(s) in that multiple 
            // requests can be made in parallel for different topicName(s).
            using (await _lock.LockAsync(cancellationToken).ConfigureAwait(false)) {
                if (topicName != null) {
                    Log.DebugFormat("BrokerRouter refreshing metadata for topic {0}", topicName);
                } else {
                    Log.DebugFormat("BrokerRouter refreshing metadata for all topics");
                }
                var response = await GetTopicMetadataFromServerAsync(new [] { topicName }, cancellationToken);
                UpdateConnectionCache(response);
                UpdateTopicCache(response);
            }
        }

        private async Task<MetadataResponse> GetTopicMetadataFromServerAsync(IEnumerable<string> topicNames, CancellationToken cancellationToken)
        {
            using (var cancellation = new TimedCancellation(cancellationToken, CacheConfiguration.RefreshRetry.Timeout)) {
                var token = cancellation.Token;
                return await CacheConfiguration.RefreshRetry.AttemptAsync(
                    (attempt, timer) => new MetadataRequest(topicNames).GetAsync(_allConnections.Values, Log, token), cancellationToken);
            }
        }

        private CachedTopicsResult TryGetCachedTopics(IEnumerable<string> topicNames, TimeSpan? expiration = null)
        {
            var missing = new List<string>();
            var topics = new List<MetadataTopic>();

            foreach (var topicName in topicNames) {
                var topic = TryGetCachedTopic(topicName, expiration);
                if (topic != null) {
                    topics.Add(topic);
                } else {
                    missing.Add(topicName);
                }
            }

            return new CachedTopicsResult(topics, missing);
        }

        private MetadataTopic GetCachedTopic(string topicName, TimeSpan? expiration = null)
        {
            var topic = TryGetCachedTopic(topicName, expiration);
            if (topic != null) return topic;

            throw new CachedMetadataException($"No metadata defined for topic/{topicName}") { Topic = topicName };
        }

        private MetadataTopic TryGetCachedTopic(string topicName, TimeSpan? expiration = null)
        {
            Tuple<MetadataTopic, DateTime> cachedTopic;
            if (_topicCache.TryGetValue(topicName, out cachedTopic)) {
                if (!expiration.HasValue || DateTime.UtcNow - cachedTopic.Item2 < expiration.Value) {
                    return cachedTopic.Item1;
                }
            }
            return null;
        }

        private BrokerRoute GetCachedRoute(string topicName, MetadataPartition partition)
        {
            var route = TryGetCachedRoute(topicName, partition);
            if (route != null) return route;

            throw new CachedMetadataException($"Lead broker cannot be found for partition/{partition.PartitionId}, leader {partition.LeaderId}") {
                Topic = topicName,
                Partition = partition.PartitionId
            };
        }

        private BrokerRoute TryGetCachedRoute(string topicName, MetadataPartition partition)
        {
            IConnection conn;
            return _brokerConnections.TryGetValue(partition.LeaderId, out conn)
                ? new BrokerRoute(topicName, partition.PartitionId, conn) 
                : null;
        }

        private CachedMetadataException GetPartitionElectionException(IList<Topic> partitionElections)
        {
            var topic = partitionElections.FirstOrDefault();
            if (topic == null) return null;

            var message = $"Leader Election for topic {topic.TopicName} partition {topic.PartitionId}";
            var innerException = GetPartitionElectionException(partitionElections.Skip(1).ToList());
            var exception = innerException != null
                                ? new CachedMetadataException(message, innerException)
                                : new CachedMetadataException(message);
            exception.Topic = topic.TopicName;
            exception.Partition = topic.PartitionId;
            return exception;
        }

        private void UpdateTopicCache(MetadataResponse metadata)
        {
            var partitionElections = metadata.Topics.SelectMany(
                t => t.Partitions
                      .Where(p => p.IsElectingLeader)
                      .Select(p => new Topic(t.TopicName, p.PartitionId)))
                      .ToList();
            if (partitionElections.Any()) throw GetPartitionElectionException(partitionElections);

            var topicCache = _topicCache;
            try {
                foreach (var topic in metadata.Topics) {
                    topicCache = topicCache.SetItem(topic.TopicName, new Tuple<MetadataTopic, DateTime>(topic, DateTime.UtcNow));
                }
            } finally {
                _topicCache = topicCache;
            }
        }

        private void UpdateConnectionCache(MetadataResponse metadata)
        {
            var allConnections = _allConnections;
            var brokerConnections = _brokerConnections;
            var connectionsToDispose = ImmutableList<IConnection>.Empty;
            try {
                foreach (var broker in metadata.Brokers) {
                    var endpoint = _connectionFactory.Resolve(broker.Address, Log);

                    IConnection connection;
                    if (brokerConnections.TryGetValue(broker.BrokerId, out connection)) {
                        if (connection.Endpoint.Equals(endpoint)) {
                            // existing connection, nothing to change
                        } else {
                            Log.WarnFormat("Broker {0} Uri changed from {1} to {2}", broker.BrokerId, connection.Endpoint, endpoint);
                            
                            // A connection changed for a broker, so close the old connection and create a new one
                            connectionsToDispose = connectionsToDispose.Add(connection);
                            connection = _connectionFactory.Create(endpoint, Configuration, Log);
                            // important that we create it here rather than set to null or we'll get it again from allConnections
                        }
                    }

                    if (connection == null && !allConnections.TryGetValue(endpoint, out connection)) {
                        connection = _connectionFactory.Create(endpoint, Configuration, Log);
                    }

                    allConnections = allConnections.SetItem(endpoint, connection);
                    brokerConnections = brokerConnections.SetItem(broker.BrokerId, connection);
                }
            } finally {
                _allConnections = allConnections;
                _brokerConnections = brokerConnections;
                DisposeConnections(connectionsToDispose);
            }
        }

        private void DisposeConnections(IEnumerable<IConnection> connections)
        {
            foreach (var connection in connections) {
                using (connection) {
                }
            }
        }

        private int _disposeCount;

        public void Dispose()
        {
            if (Interlocked.Increment(ref _disposeCount) != 1) return;
            DisposeConnections(_allConnections.Values);
        }

        public ILog Log { get; }

        private class CachedTopicsResult
        {
            public ImmutableList<MetadataTopic> Topics { get; }
            public ImmutableList<string> Missing { get; }

            public CachedTopicsResult(IEnumerable<MetadataTopic> topics, IEnumerable<string> missing)
            {
                Topics = ImmutableList<MetadataTopic>.Empty.AddNotNullRange(topics);
                Missing = ImmutableList<string>.Empty.AddNotNullRange(missing);
            }
        }
    }
}