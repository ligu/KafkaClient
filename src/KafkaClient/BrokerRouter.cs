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
        private readonly KafkaOptions _options;
        private readonly KafkaMetadataProvider _kafkaMetadataProvider;

        private ImmutableDictionary<KafkaEndpoint, IKafkaConnection> _allConnections = ImmutableDictionary<KafkaEndpoint, IKafkaConnection>.Empty;
        private ImmutableDictionary<int, IKafkaConnection> _brokerConnections = ImmutableDictionary<int, IKafkaConnection>.Empty;
        private ImmutableDictionary<string, Tuple<MetadataTopic, DateTime>> _topicCache = ImmutableDictionary<string, Tuple<MetadataTopic, DateTime>>.Empty;

        private readonly AsyncLock _lock = new AsyncLock();

        /// <exception cref="KafkaConnectionException">None of the provided Kafka servers are resolvable.</exception>
        public BrokerRouter(KafkaOptions options)
        {
            _options = options;

            foreach (var endpoint in _options.KafkaServerEndpoints) {
                _allConnections = _allConnections.SetItem(endpoint, _options.Create(endpoint));
            }
            if (_allConnections.IsEmpty) throw new KafkaConnectionException("None of the provided Kafka servers are resolvable.");

            _kafkaMetadataProvider = new KafkaMetadataProvider(_options.Log);
        }

        /// <summary>
        /// Select a broker for a specific topic and partitionId.
        /// </summary>
        /// <param name="topicName">The topic name to select a broker for.</param>
        /// <param name="partitionId">The exact partition to select a broker for.</param>
        /// <returns>A broker route for the given partition of the given topic.</returns>
        /// <remarks>
        /// This function does not use any selector criteria.  If the given partitionId does not exist an exception will be thrown.
        /// </remarks>
        /// <exception cref="CachedMetadataException">Thrown if the given topic or partitionId does not exist for the given topic.</exception>
        public BrokerRoute SelectBrokerRouteFromLocalCache(string topicName, int partitionId)
        {
            var topic = GetCachedTopic(topicName);

            var partition = topic.Partitions.FirstOrDefault(x => x.PartitionId == partitionId);
            if (partition == null) throw new CachedMetadataException($"The topic: {topicName} has no partitionId: {partitionId} defined.") { Topic = topicName, Partition = partitionId };

            return GetCachedRoute(topicName, partition);
        }

        /// <summary>
        /// Select a broker for a given topic using the IPartitionSelector function.
        /// </summary>
        /// <param name="topicName">The topic to retreive a broker route for.</param>
        /// <param name="key">The key used by the IPartitionSelector to collate to a consistent partition. Null value means key will be ignored in selection process.</param>
        /// <returns>A broker route for the given topic.</returns>
        /// <exception cref="CachedMetadataException">Thrown if the topic metadata does not exist in the cache.</exception>
        public BrokerRoute SelectBrokerRouteFromLocalCache(string topicName, byte[] key = null)
        {
            var topic = GetCachedTopic(topicName);
            return GetCachedRoute(topicName, _options.PartitionSelector.Select(topic, key));
        }

        /// <summary>
        /// Returns Topic metadata for the given topic.
        /// </summary>
        /// <returns>List of Topics currently in the cache.</returns>
        /// <remarks>
        /// The topic metadata returned is from what is currently in the cache. To ensure data is not too stale, call 
        /// <see cref="RefreshMissingTopicMetadataAsync(string, CancellationToken)"/> first or 
        /// use <see cref="GetTopicMetadataAsync(string, CancellationToken)"/>.
        /// </remarks>
        /// <exception cref="CachedMetadataException">Thrown if the topic metadata does not exist in the cache.</exception>
        public MetadataTopic GetTopicMetadataFromLocalCache(string topicName)
        {
            return GetCachedTopic(topicName);
        }

        /// <summary>
        /// Returns Topic metadata for each topic requested.
        /// </summary>
        /// <returns>List of Topics currently in the cache.</returns>
        /// <remarks>
        /// The topic metadata returned is from what is currently in the cache. To ensure data is not too stale, call 
        /// <see cref="RefreshMissingTopicMetadataAsync(IEnumerable&lt;string&gt;, CancellationToken)"/> first or 
        /// use <see cref="GetTopicMetadataAsync(IEnumerable&lt;string&gt;, CancellationToken)"/>.
        /// </remarks>
        /// <exception cref="CachedMetadataException">Thrown if the topic metadata does not exist in the cache.</exception>
        public ImmutableList<MetadataTopic> GetTopicMetadataFromLocalCache(IEnumerable<string> topicNames)
        {
            var topicSearchResult = TryGetCachedTopics(topicNames, _options.CacheExpiration);
            if (topicSearchResult.Missing.Count > 0) throw new CachedMetadataException($"No metadata defined for topics: {string.Join(",", topicSearchResult.Missing)}");

            return ImmutableList<MetadataTopic>.Empty.AddRange(topicSearchResult.Topics);
        }

        /// <summary>
        /// Returns all cached topic metadata.
        /// </summary>
        public ImmutableList<MetadataTopic> GetTopicMetadataFromLocalCache()
        {
            return ImmutableList<MetadataTopic>.Empty.AddRange(_topicCache.Values.Select(t => t.Item1));
        }

        /// <summary>
        /// Returns Topic metadata for the topic requested.
        /// </summary>
        /// <remarks>
        /// This method will check the cache first, and if the topic is missing it will initiate a call to the kafka 
        /// servers, updating the cache with the resulting metadata.
        /// </remarks>
        public async Task<MetadataTopic> GetTopicMetadataAsync(string topicName, CancellationToken cancellationToken)
        {
            return TryGetCachedTopic(topicName, _options.CacheExpiration) 
                ?? await UpdateTopicMetadataFromServerIfMissingAsync(topicName, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Returns Topic metadata for each topic requested.
        /// </summary>
        /// <remarks>
        /// This method will check the cache first, and for any missing topic metadata it will initiate a call to the kafka 
        /// servers, updating the cache with the resulting metadata.
        /// </remarks>
        public async Task<ImmutableList<MetadataTopic>> GetTopicMetadataAsync(IEnumerable<string> topicNames, CancellationToken cancellationToken)
        {
            var searchResult = TryGetCachedTopics(topicNames, _options.CacheExpiration);
            return searchResult.Missing.IsEmpty 
                ? searchResult.Topics 
                : searchResult.Topics.AddRange(await UpdateTopicMetadataFromServerIfMissingAsync(searchResult.Missing, cancellationToken).ConfigureAwait(false));
        }

        /// <summary>
        /// Refresh metadata for the given topic if it isn't present in the cache.
        /// </summary>
        /// <remarks>
        /// This method will check the cache first, and if it doesn't find the topic will initiate a call to the kafka servers, updating the cache with the resulting metadata.
        /// </remarks>
        public async Task<bool> RefreshMissingTopicMetadataAsync(string topicName, CancellationToken cancellationToken)
        {
            var topic = TryGetCachedTopic(topicName, _options.CacheExpiration);
            if (topic != null) return false;

            await UpdateTopicMetadataFromServerIfMissingAsync(topicName, cancellationToken).ConfigureAwait(false);
            return true;
        }

        /// <summary>
        /// Force a call to the kafka servers to refresh metadata for the given topics.
        /// </summary>
        /// <remarks>
        /// This method will initiate a call to the kafka servers and retrieve metadata for all given topics, updating the broker cache in the process.
        /// </remarks>
        public async Task<bool> RefreshMissingTopicMetadataAsync(IEnumerable<string> topicNames, CancellationToken cancellationToken)
        {
            var searchResult = TryGetCachedTopics(topicNames, _options.CacheExpiration);
            if (searchResult.Missing.IsEmpty) return false;

            await UpdateTopicMetadataFromServerIfMissingAsync(searchResult.Missing, cancellationToken).ConfigureAwait(false);
            return true;
        }

        /// <summary>
        /// Force a call to the kafka servers to refresh metadata for the given topic.
        /// </summary>
        /// <remarks>
        /// This method will ignore the cache and initiate a call to the kafka servers for the given topic, updating the cache with the resulting metadata.
        /// </remarks>
        public Task RefreshTopicMetadataAsync(string topicName, CancellationToken cancellationToken)
        {
            return UpdateTopicMetadataFromServerAsync(topicName, cancellationToken);
        }

        /// <summary>
        /// Force a call to the kafka servers to refresh metadata for all topics.
        /// </summary>
        /// <remarks>
        /// This method will ignore the cache and initiate a call to the kafka servers for all topics, updating the cache with the resulting metadata.
        /// </remarks>
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
            using (await _lock.LockAsync(cancellationToken).ConfigureAwait(false)) {
                var searchResult = TryGetCachedTopics(topicNames, _options.CacheExpiration);
                if (searchResult.Missing.Count == 0) return searchResult.Topics;

                _options.Log.DebugFormat("BrokerRouter refreshing metadata for topics {0}", string.Join(",", searchResult.Missing));
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
            using (await _lock.LockAsync(cancellationToken).ConfigureAwait(false)) {
                if (topicName != null) {
                    _options.Log.DebugFormat("BrokerRouter refreshing metadata for topic {0}", topicName);
                } else {
                    _options.Log.DebugFormat("BrokerRouter refreshing metadata for all topics");
                }
                var response = await GetTopicMetadataFromServerAsync(new [] { topicName }, cancellationToken);
                UpdateConnectionCache(response);
                UpdateTopicCache(response);
            }
        }

        private async Task<MetadataResponse> GetTopicMetadataFromServerAsync(IEnumerable<string> topicNames, CancellationToken cancellationToken)
        {
            using (var cancellation = new TimedCancellation(cancellationToken, _options.RefreshMetadataTimeout)) {
                var requestTask = topicNames != null
                        ? _kafkaMetadataProvider.GetAsync(_allConnections.Values, topicNames, cancellation.Token)
                        : _kafkaMetadataProvider.GetAsync(_allConnections.Values, cancellation.Token);
                return await requestTask.ConfigureAwait(false);
            }
        }

        private CachedTopicsResult TryGetCachedTopics(IEnumerable<string> topicNames, TimeSpan expiration)
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

            throw new CachedMetadataException($"No metadata defined for topic: {topicName}") { Topic = topicName };
        }

        private MetadataTopic TryGetCachedTopic(string topicName, TimeSpan? expiration)
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

            throw new CachedMetadataException($"Lead broker cannot be found for partition: {partition.PartitionId}, leader: {partition.LeaderId}") {
                Topic = topicName,
                Partition = partition.PartitionId
            };
        }

        private BrokerRoute TryGetCachedRoute(string topicName, MetadataPartition partition)
        {
            IKafkaConnection conn;
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
            var connectionsToDispose = ImmutableList<IKafkaConnection>.Empty;
            try {
                foreach (var broker in metadata.Brokers) {
                    var endpoint = _options.KafkaConnectionFactory.Resolve(broker.Address, _options.Log);

                    IKafkaConnection connection;
                    if (brokerConnections.TryGetValue(broker.BrokerId, out connection)) {
                        if (connection.Endpoint.Equals(endpoint)) {
                            // existing connection, nothing to change
                        } else {
                            _options.Log.WarnFormat("Broker {0} Uri changed from {1} to {2}", broker.BrokerId, connection.Endpoint, endpoint);
                            
                            // A connection changed for a broker, so close the old connection and create a new one
                            connectionsToDispose = connectionsToDispose.Add(connection);
                            connection = _options.Create(endpoint);
                            // important that we create it here rather than set to null or we'll get it again from allConnections
                        }
                    }

                    if (connection == null && !allConnections.TryGetValue(endpoint, out connection)) {
                        connection = _options.Create(endpoint);
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

        private void DisposeConnections(IEnumerable<IKafkaConnection> connections)
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

        public IKafkaLog Log => _options.Log;

        private class CachedTopicsResult
        {
            public ImmutableList<MetadataTopic> Topics { get; }
            public ImmutableList<string> Missing { get; }

            public CachedTopicsResult(IEnumerable<MetadataTopic> topics, IEnumerable<string> missing)
            {
                Topics = topics != null
                             ? ImmutableList<MetadataTopic>.Empty.AddRange(topics)
                             : ImmutableList<MetadataTopic>.Empty;
                Missing = missing != null ? ImmutableList<string>.Empty.AddRange(missing) : ImmutableList<string>.Empty;
            }
        }
    }
}