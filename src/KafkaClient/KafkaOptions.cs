using System;
using System.Collections.Generic;
using System.Linq;
using KafkaClient.Common;
using KafkaClient.Connection;

namespace KafkaClient
{
    public class KafkaOptions
    {
        public KafkaOptions(params Uri[] kafkaServerUri)
        {
            KafkaServerUri = kafkaServerUri.ToList();
            PartitionSelector = new DefaultPartitionSelector();
            Log = new TraceLog();
            KafkaConnectionFactory = new KafkaConnectionFactory();
            ResponseTimeoutMs = TimeSpan.FromMilliseconds(DefaultResponseTimeout);
            CacheExpiration = TimeSpan.FromMilliseconds(DefaultCacheExpirationTimeoutMS);
            RefreshMetadataTimeout = TimeSpan.FromMilliseconds(DefaultRefreshMetadataTimeout);
            MaxRetry = DefaultMaxRetry;
            StatisticsTrackerOptions = new StatisticsTrackerOptions();
        }

        private const int DefaultResponseTimeout = 60000;
        private const int DefaultCacheExpirationTimeoutMS = 10;
        private const int DefaultRefreshMetadataTimeout = 200000;
        private const int DefaultMaxRetry = 5;

        /// <summary>
        /// Refresh metadata Request will try to refresh only the topics that were expired in the cache.
        /// </summary>

        public StatisticsTrackerOptions StatisticsTrackerOptions { get; }
        public TimeSpan CacheExpiration { get; set; }
        public TimeSpan RefreshMetadataTimeout { get; set; }
        public int MaxRetry { get; set; }

        /// <summary>
        /// List of Uri connections to kafka servers.  The are used to query for metadata from Kafka.  More than one is recommended.
        /// </summary>
        public List<Uri> KafkaServerUri { get; set; }

        /// <summary>
        /// Safely attempts to resolve endpoints from the KafkaServerUri, ignoreing all resolvable ones.
        /// </summary>
        public IEnumerable<KafkaEndpoint> KafkaServerEndpoints
        {
            get
            {
                foreach (var uri in KafkaServerUri) {
                    KafkaEndpoint endpoint = null;
                    try {
                        endpoint = KafkaConnectionFactory.Resolve(uri, Log);
                    } catch (KafkaConnectionException ex) {
                        Log.WarnFormat(ex, "Ignoring uri that could not be resolved: {0}", uri);
                    }

                    if (endpoint != null) yield return endpoint;
                }
            }
        }

        /// <summary>
        /// Provides a factory for creating new kafka connections.
        /// </summary>
        public IKafkaConnectionFactory KafkaConnectionFactory { get; set; }

        /// <summary>
        /// Selector function for routing messages to partitions. Default is key/hash and round robin.
        /// </summary>
        public IPartitionSelector PartitionSelector { get; set; }

        /// <summary>
        /// Timeout length in milliseconds waiting for a response from kafka.
        /// </summary>
        public TimeSpan ResponseTimeoutMs { get; set; }

        /// <summary>
        /// Log object to record operational messages.
        /// </summary>
        public IKafkaLog Log { get; set; }

        /// <summary>
        /// The maximum time to wait when backing off on reconnection attempts.
        /// </summary>
        public TimeSpan? MaximumReconnectionTimeout { get; set; }
    }
}