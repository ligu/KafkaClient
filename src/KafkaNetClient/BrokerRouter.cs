using KafkaNet.Common;
using KafkaNet.Model;
using KafkaNet.Protocol;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace KafkaNet
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
        private readonly KafkaOptions _kafkaOptions;
        private readonly KafkaMetadataProvider _kafkaMetadataProvider;
        private readonly ConcurrentDictionary<KafkaEndpoint, IKafkaConnection> _defaultConnectionIndex = new ConcurrentDictionary<KafkaEndpoint, IKafkaConnection>();
        private readonly ConcurrentDictionary<int, IKafkaConnection> _brokerConnectionIndex = new ConcurrentDictionary<int, IKafkaConnection>();
        private readonly ConcurrentDictionary<string, Tuple<Topic, DateTime>> _topicIndex = new ConcurrentDictionary<string, Tuple<Topic, DateTime>>();
        private readonly AsyncLock _taskLocker = new AsyncLock();

        /// <exception cref="ServerUnreachableException">None of the provided Kafka servers are resolvable.</exception>

        public BrokerRouter(KafkaOptions kafkaOptions)
        {
            _kafkaOptions = kafkaOptions;
            _kafkaMetadataProvider = new KafkaMetadataProvider(_kafkaOptions.Log);

            foreach (var endpoint in _kafkaOptions.KafkaServerEndpoints)
            {
                var conn = _kafkaOptions.KafkaConnectionFactory.Create(endpoint, _kafkaOptions.ResponseTimeoutMs, _kafkaOptions.Log, _kafkaOptions.MaxRetry, _kafkaOptions.MaximumReconnectionTimeout, kafkaOptions.StatisticsTrackerOptions);
                _defaultConnectionIndex.AddOrUpdate(endpoint, e => conn, (e, c) => conn);
            }

            if (_defaultConnectionIndex.Count <= 0)
                throw new KafkaConnectionException("None of the provided Kafka servers are resolvable.");
        }

        private IKafkaConnection[] GetConnections()
        {
            return _defaultConnectionIndex.Values.Union(_brokerConnectionIndex.Values).ToArray();
        }

        /// <summary>
        /// Select a broker for a specific topic and partitionId.
        /// </summary>
        /// <param name="topic">The topic name to select a broker for.</param>
        /// <param name="partitionId">The exact partition to select a broker for.</param>
        /// <returns>A broker route for the given partition of the given topic.</returns>
        /// <remarks>
        /// This function does not use any selector criteria.  If the given partitionId does not exist an exception will be thrown.
        /// </remarks>
        /// <exception cref="InvalidPartitionException">Thrown if the give partitionId does not exist for the given topic.</exception>
        /// <exception cref="InvalidTopicNotExistsInCache">Thrown if the topic metadata does not exist in the cache.</exception>

        public BrokerRoute SelectBrokerRouteFromLocalCache(string topic, int partitionId)
        {
            var cachedTopic = GetTopicMetadataFromLocalCache(topic);
            var topicMetadata = cachedTopic.First();
            if (topicMetadata == null)
            {
                throw new CachedMetadataException($"No metadata defined for topic: {topic}") { Topic = topic };
            }

            var partition = topicMetadata.Partitions.FirstOrDefault(x => x.PartitionId == partitionId);
            if (partition == null)
            {
                throw new CachedMetadataException($"The topic: {topic} has no partitionId: {partitionId} defined.") { Topic = topic, Partition = partitionId };
            }

            return GetCachedRoute(topicMetadata.Name, partition);
        }

        /// <summary>
        /// Select a broker for a given topic using the IPartitionSelector function.
        /// </summary>
        /// <param name="topic">The topic to retreive a broker route for.</param>
        /// <param name="key">The key used by the IPartitionSelector to collate to a consistent partition. Null value means key will be ignored in selection process.</param>
        /// <returns>A broker route for the given topic.</returns>
        /// <exception cref="InvalidTopicNotExistsInCache">Thrown if the topic metadata does not exist in the cache.</exception>
        public BrokerRoute SelectBrokerRouteFromLocalCache(string topic, byte[] key = null)
        {
            //get topic either from cache or server.
            var cachedTopic = GetTopicMetadataFromLocalCache(topic).FirstOrDefault();

            if (cachedTopic == null)
            {
                throw new CachedMetadataException($"No metadata defined for topic: {topic}") { Topic = topic };
            }

            var partition = _kafkaOptions.PartitionSelector.Select(cachedTopic, key);

            return GetCachedRoute(cachedTopic.Name, partition);
        }

        /// <summary>
        /// Returns Topic metadata for each topic requested.
        /// </summary>
        /// <param name="topics">Collection of topics to request metadata for.</param>
        /// <returns>List of Topics as provided by Kafka.</returns>
        /// <remarks>
        /// The topic metadata will by default check the cache first and then if it does not exist it will then
        /// request metadata from the server.  To force querying the metadata from the server use <see cref="RefreshTopicMetadata"/>
        /// </remarks>
        /// <exception cref="InvalidTopicNotExistsInCache">Thrown if the topic metadata does not exist in the cache.</exception>
        public List<Topic> GetTopicMetadataFromLocalCache(params string[] topics)
        {
            var topicSearchResult = SearchCacheForTopics(topics, null);

            if (topicSearchResult.Missing.Count > 0)
            {
                throw new CachedMetadataException($"No metadata defined for topics: {string.Join(",", topicSearchResult.Missing)}");
            }

            return topicSearchResult.Topics;
        }

        /// <summary>
        /// Force a call to the kafka servers to refresh metadata for the given topics.
        /// </summary>
        /// <param name="topics">List of topics to update metadata for.</param>
        /// <remarks>
        /// This method will ignore the cache and initiate a call to the kafka servers for all given topics, updating the cache with the resulting metadata.
        /// Only call this method to force a metadata update.  For all other queries use <see cref="GetTopicMetadataFromLocalCache"/> which uses cached values.
        /// </remarks>
        public Task<bool> RefreshTopicMetadata(params string[] topics)
        {
            return RefreshTopicMetadata(_kafkaOptions.CacheExpiration, _kafkaOptions.RefreshMetadataTimeout, topics);
        }

        /// <summary>
        /// Refresh metadata Request will try to refresh only the topics that were expired in the
        /// cache. If cacheExpiration is null: refresh metadata Request will try to refresh only
        /// topics that are not in the cache.
        /// </summary>
        private async Task<bool> RefreshTopicMetadata(TimeSpan? cacheExpiration, TimeSpan timeout, params string[] topics)
        {
            using (await _taskLocker.LockAsync().ConfigureAwait(false))
            {
                int missingFromCache = SearchCacheForTopics(topics, cacheExpiration).Missing.Count;
                if (missingFromCache == 0)
                {
                    return false;
                }

                _kafkaOptions.Log.DebugFormat("BrokerRouter: Refreshing metadata for topics: {0}", string.Join(",", topics));

                var connections = GetConnections();
                var metadataRequestTask = _kafkaMetadataProvider.Get(connections, topics);
                var metadataResponse = await metadataRequestTask.WithTimeout(timeout).ConfigureAwait(false);

                UpdateInternalMetadataCache(metadataResponse);
            }
            return true;
        }

        /// <summary>
        /// Returns Topic metadata for each topic.
        /// </summary>
        /// <returns>List of Topics as provided by Kafka.</returns>
        /// <remarks>
        /// The topic metadata will by default check the cache. To force querying the metadata from the server use <see cref="RefreshAllTopicMetadata"/>
        /// </remarks>
        public List<Topic> GetAllTopicMetadataFromLocalCache()
        {
            return _topicIndex.Values.Select(t => t.Item1).ToList();
        }

        /// <summary>
        /// Force a call to the kafka servers to refresh metadata for all topics.
        /// </summary>
        /// <remarks>
        /// This method will ignore the cache and initiate a call to the kafka servers for all topics, updating the cache with the resulting metadata.
        /// Only call this method to force a metadata update. For all other queries use <see cref="GetAllTopicMetadataFromLocalCache"/> which uses cached values.
        /// </remarks>
        public Task RefreshAllTopicMetadata()
        {
            return RefreshAllTopicMetadata(_kafkaOptions.RefreshMetadataTimeout);
        }

        /// <summary>
        /// Force refresh each topic metadata that exists on kafka servers.
        /// </summary>
        private async Task RefreshAllTopicMetadata(TimeSpan timeout)
        {
            using (await _taskLocker.LockAsync().ConfigureAwait(false))
            {
                _kafkaOptions.Log.DebugFormat("BrokerRouter: Refreshing metadata for all topics");

                var connections = GetConnections();
                var metadataRequestTask = _kafkaMetadataProvider.Get(connections);
                var metadataResponse = await metadataRequestTask.WithTimeout(timeout).ConfigureAwait(false);

                UpdateInternalMetadataCache(metadataResponse);
            }
        }

        private TopicSearchResult SearchCacheForTopics(IEnumerable<string> topics, TimeSpan? expiration)
        {
            var result = new TopicSearchResult();

            foreach (var topic in topics)
            {
                var cachedTopic = GetCachedTopic(topic, expiration);

                if (cachedTopic == null)
                {
                    result.Missing.Add(topic);
                }
                else
                {
                    result.Topics.Add(cachedTopic);
                }
            }

            return result;
        }

        private Topic GetCachedTopic(string topic, TimeSpan? expiration = null)
        {
            Tuple<Topic, DateTime> cachedTopic;
            if (_topicIndex.TryGetValue(topic, out cachedTopic))
            {
                bool hasExpirationPolicy = expiration.HasValue;
                bool isNotExpired = expiration.HasValue && (DateTime.Now - cachedTopic.Item2).TotalMilliseconds < expiration.Value.TotalMilliseconds;
                if (!hasExpirationPolicy || isNotExpired)
                {
                    return cachedTopic.Item1;
                }
            }
            return null;
        }

        private BrokerRoute GetCachedRoute(string topic, Partition partition)
        {
            var route = TryGetRouteFromCache(topic, partition);
            if (route != null) return route;

            throw new CachedMetadataException($"Lead broker cannot be found for partition: {partition.PartitionId}, leader: {partition.LeaderId}") {
                Topic = topic,
                Partition = partition.PartitionId
            };
        }

        private BrokerRoute TryGetRouteFromCache(string topic, Partition partition)
        {
            IKafkaConnection conn;
            if (_brokerConnectionIndex.TryGetValue(partition.LeaderId, out conn))
            {
                return new BrokerRoute
                {
                    Topic = topic,
                    PartitionId = partition.PartitionId,
                    Connection = conn
                };
            }

            return null;
        }

        private void UpdateInternalMetadataCache(MetadataResponse metadata)
        {
            var noLeaderElectedForPartition =
                metadata.Topics.Select(x => new {topic = x.Name, partition = x.Partitions.FirstOrDefault(i => i.LeaderId == -1)})
                    .FirstOrDefault(x => x.partition != null);

            if (noLeaderElectedForPartition != null)
                throw new CachedMetadataException($"topic:{noLeaderElectedForPartition.topic} partition:{noLeaderElectedForPartition.partition}") {
                    Topic = noLeaderElectedForPartition.topic,
                    Partition = noLeaderElectedForPartition.partition?.PartitionId
                };

            //resolve each broker
            var brokerEndpoints = metadata.Brokers.Select(broker => new
            {
                Broker = broker,
                Endpoint = _kafkaOptions.KafkaConnectionFactory.Resolve(broker.Address, _kafkaOptions.Log)
            });

            foreach (var broker in brokerEndpoints)
            {
                //if the connection is in our default connection index already, remove it and assign it to the broker index.
                IKafkaConnection connection;
                _defaultConnectionIndex.TryRemove(broker.Endpoint, out connection);

                Func<int, IKafkaConnection> connectionFactory = (i) => connection ?? _kafkaOptions.KafkaConnectionFactory.Create(broker.Endpoint, _kafkaOptions.ResponseTimeoutMs, _kafkaOptions.Log, _kafkaOptions.MaxRetry);
                UpsertConnectionToBrokerConnectionIndex(broker.Broker.BrokerId, broker.Endpoint, connectionFactory);
            }

            foreach (var topic in metadata.Topics)
            {
                var localTopic = new Tuple<Topic, DateTime>(topic, DateTime.Now);
                _topicIndex.AddOrUpdate(topic.Name, s => localTopic, (s, existing) => localTopic);
            }
        }

        private void UpsertConnectionToBrokerConnectionIndex(int brokerId, KafkaEndpoint brokerEndpoint, Func<int, IKafkaConnection> connectionFactory)
        {
            //associate the connection with the broker id, and add or update the reference
            _brokerConnectionIndex.AddOrUpdate(brokerId, connectionFactory,
                    (i, existingConnection) =>
                    {
                        //if a connection changes for a broker close old connection and create a new one
                        if (existingConnection.Endpoint.Equals(brokerEndpoint)) return existingConnection;
                        _kafkaOptions.Log.WarnFormat("Broker:{0} Uri changed from:{1} to {2}", brokerId, existingConnection.Endpoint, brokerEndpoint);
                        
                        existingConnection.Dispose();
                        return connectionFactory(i);
                    });
        }

        public void Dispose()
        {
            _defaultConnectionIndex.Values.ToList().ForEach(conn => { using (conn) { } });
            _brokerConnectionIndex.Values.ToList().ForEach(conn => { using (conn) { } });
        }

        public async Task RefreshMissingTopicMetadata(params string[] topics)
        {
            var topicSearchResult = SearchCacheForTopics(topics, null);

            //update metadata for all missing topics
            if (topicSearchResult.Missing.Count > 0)
            {
                //double check for missing topics and query
                await RefreshTopicMetadata(null, _kafkaOptions.RefreshMetadataTimeout, topicSearchResult.Missing.Where(x => _topicIndex.ContainsKey(x) == false).ToArray()).ConfigureAwait(false);
            }
        }

        public DateTime GetTopicMetadataRefreshTime(string topic)
        {
            return _topicIndex[topic].Item2;
        }

        public IKafkaLog Log
        {
            get { return _kafkaOptions.Log; }
        }
    }

    #region BrokerCache Class...

    public class TopicSearchResult
    {
        public List<Topic> Topics { get; set; }
        public List<string> Missing { get; set; }

        public TopicSearchResult()
        {
            Topics = new List<Topic>();
            Missing = new List<string>();
        }
    }

    #endregion BrokerCache Class...
}