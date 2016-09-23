using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;
using KafkaClient.Connection;

namespace KafkaClient
{
    public class KafkaOptions
    {
        public KafkaOptions(
            IEnumerable<Uri> kafkaServerUris = null, 
            IKafkaConnectionConfiguration connectionConfiguration = null, 
            ICacheConfiguration cacheConfiguration = null,
            IKafkaConnectionFactory connectionFactory = null,
            IPartitionSelector partitionSelector = null,
            IKafkaLog log = null)
        {
            ServerUris = ImmutableList<Uri>.Empty.AddNotNullRange(kafkaServerUris);
            CacheConfiguration = cacheConfiguration ?? new CacheConfiguration();
            ConnectionConfiguration = connectionConfiguration ?? new KafkaConnectionConfiguration();
            ConnectionFactory = connectionFactory ?? new KafkaConnectionFactory();
            PartitionSelector = partitionSelector ?? new PartitionSelector();
            Log = log ?? TraceLog.Log;
        }

        public KafkaOptions(
            Uri kafkaServerUri = null, 
            IKafkaConnectionConfiguration connectionConfiguration = null, 
            ICacheConfiguration cacheConfiguration = null,
            IKafkaConnectionFactory connectionFactory = null,
            IPartitionSelector partitionSelector = null,
            IKafkaLog log = null)
            : this (ImmutableList<Uri>.Empty.AddNotNull(kafkaServerUri), connectionConfiguration, cacheConfiguration, connectionFactory, partitionSelector, log)
        {
        }

        /// <summary>
        /// Initial list of Uris to kafka servers, used to query for metadata from Kafka. More than one is recommended!
        /// </summary>
        public ImmutableList<Uri> ServerUris { get; }

        /// <summary>
        /// Connection backoff and retry settings.
        /// </summary>
        public IKafkaConnectionConfiguration ConnectionConfiguration { get; }

        /// <summary>
        /// Cache expiry and retry settings.
        /// </summary>
        public ICacheConfiguration CacheConfiguration { get; }

        /// <summary>
        /// Provides a factory for creating new kafka connections.
        /// </summary>
        public IKafkaConnectionFactory ConnectionFactory { get; }

        /// <summary>
        /// Selector function for routing messages to partitions. Default is key/hash and round robin.
        /// </summary>
        public IPartitionSelector PartitionSelector { get; }

        /// <summary>
        /// Log object to record operational messages.
        /// </summary>
        public IKafkaLog Log { get; }
    }
}