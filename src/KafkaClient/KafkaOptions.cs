using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;
using KafkaClient.Connection;

namespace KafkaClient
{
    public class KafkaOptions : KafkaConnectionOptions
    {
        public KafkaOptions(IEnumerable<Uri> kafkaServerUris = null, TimeSpan? connectingTimeout = null, int? maxRetries = null, TimeSpan? requestTimeout = null, bool trackTelemetry = false)
            : base (connectingTimeout, maxRetries, requestTimeout, trackTelemetry)
        {
            KafkaServerUris = ImmutableList<Uri>.Empty.AddRangeNotNull(kafkaServerUris);
            PartitionSelector = new PartitionSelector();
            Log = new TraceLog();
            KafkaConnectionFactory = new KafkaConnectionFactory();
        }

        public KafkaOptions(Uri kafkaServerUri = null, TimeSpan? connectingTimeout = null, int? maxRetries = null, TimeSpan? requestTimeout = null, bool trackTelemetry = false)
            : this (ImmutableList<Uri>.Empty.AddNotNull(kafkaServerUri), connectingTimeout, maxRetries, requestTimeout, trackTelemetry)
        {
        }

        /// <summary>
        /// Refresh metadata Request will try to refresh only the topics that were expired in the cache.
        /// </summary>
        public TimeSpan CacheExpiration { get; set; }
        public TimeSpan RefreshMetadataTimeout { get; set; }

        /// <summary>
        /// List of Uri connections to kafka servers.  The are used to query for metadata from Kafka.  More than one is recommended.
        /// </summary>
        public ImmutableList<Uri> KafkaServerUris { get; set; }

        /// <summary>
        /// Provides a factory for creating new kafka connections.
        /// </summary>
        public IKafkaConnectionFactory KafkaConnectionFactory { get; set; }

        /// <summary>
        /// Selector function for routing messages to partitions. Default is key/hash and round robin.
        /// </summary>
        public IPartitionSelector PartitionSelector { get; set; }

        /// <summary>
        /// Log object to record operational messages.
        /// </summary>
        public IKafkaLog Log { get; set; }
    }
}