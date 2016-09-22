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
            IKafkaConnectionOptions connectionOptions = null, 
            IMetadataCacheOptions cacheOptions = null,
            IKafkaConnectionFactory connectionFactory = null,
            IPartitionSelector partitionSelector = null,
            IKafkaLog log = null)
        {
            ServerUris = ImmutableList<Uri>.Empty.AddRangeNotNull(kafkaServerUris);
            CacheOptions = cacheOptions;
            ConnectionOptions = connectionOptions;
            ConnectionFactory = connectionFactory ?? new KafkaConnectionFactory();
            PartitionSelector = partitionSelector ?? new PartitionSelector();
            Log = log ?? new TraceLog();
        }

        public KafkaOptions(
            Uri kafkaServerUri = null, 
            IKafkaConnectionOptions connectionOptions = null, 
            IMetadataCacheOptions cacheOptions = null,
            IKafkaConnectionFactory connectionFactory = null,
            IPartitionSelector partitionSelector = null,
            IKafkaLog log = null)
            : this (ImmutableList<Uri>.Empty.AddNotNull(kafkaServerUri), connectionOptions, cacheOptions, connectionFactory, partitionSelector, log)
        {
        }

        /// <summary>
        /// Initial list of Uris to kafka servers, used to query for metadata from Kafka. More than one is recommended!
        /// </summary>
        public ImmutableList<Uri> ServerUris { get; }

        /// <summary>
        /// Connection backoff and retry settings.
        /// </summary>
        public IKafkaConnectionOptions ConnectionOptions { get; }

        /// <summary>
        /// Cache expiry and retry settings.
        /// </summary>
        public IMetadataCacheOptions CacheOptions { get; }

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