using System;
using System.Collections.Generic;
using System.Linq;
using KafkaNet.Common;

namespace KafkaNet.Protocol
{
    public static class DecodeResponse
    {
        /// <summary>
        /// ProduceResponse => [TopicName [Partition ErrorCode Offset *Timestamp]] *ThrottleTime
        ///  *ThrottleTime is only version 1 (0.9.0) and above
        ///  *Timestamp is only version 2 (0.10.0) and above
        ///  TopicName => string   -- The topic this response entry corresponds to.
        ///  Partition => int32    -- The partition this response entry corresponds to.
        ///  ErrorCode => int16    -- The error from this partition, if any. Errors are given on a per-partition basis because a given partition may be 
        ///                           unavailable or maintained on a different host, while others may have successfully accepted the produce request.
        ///  Offset => int64       -- The offset assigned to the first message in the message set appended to this partition.
        ///  Timestamp => int64    -- If LogAppendTime is used for the topic, this is the timestamp assigned by the broker to the message set. 
        ///                           All the messages in the message set have the same timestamp.
        ///                           If CreateTime is used, this field is always -1. The producer can assume the timestamp of the messages in the 
        ///                           produce request has been accepted by the broker if there is no error code returned.
        ///                           Unit is milliseconds since beginning of the epoch (midnight Jan 1, 1970 (UTC)).
        ///  ThrottleTime => int32 -- Duration in milliseconds for which the request was throttled due to quota violation. 
        ///                           (Zero if the request did not violate any quota).
        /// 
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets
        /// </summary>
        public static ProduceResponse ProduceResponse(short version, byte[] data)
        {
            using (var stream = new BigEndianBinaryReader(data)) {
                var correlationId = stream.ReadInt32();
                TimeSpan? throttleTime = null;

                var topics = new List<ProduceTopic>();
                var topicCount = stream.ReadInt32();
                for (int i = 0; i < topicCount; i++) {
                    var topicName = stream.ReadInt16String();

                    var partitionCount = stream.ReadInt32();
                    for (int j = 0; j < partitionCount; j++) {
                        var partitionId = stream.ReadInt32();
                        var errorCode = (ErrorResponseCode) stream.ReadInt16();
                        var offset = stream.ReadInt64();
                        DateTime? timestamp = null;

                        if (version >= 2) {
                            var milliseconds = stream.ReadInt64();
                            if (milliseconds >= 0) {
                                timestamp = milliseconds.FromUnixEpochMilliseconds();
                            }
                        }

                        topics.Add(new ProduceTopic(topicName, partitionId, errorCode, offset, timestamp));
                    }
                }

                if (version >= 1) {
                    throttleTime = TimeSpan.FromMilliseconds(stream.ReadInt32());
                }
                return new ProduceResponse(correlationId, topics, throttleTime);
            }
        }

        /// <summary>
        /// FetchResponse => *ThrottleTime [TopicName [Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet]]
        ///  *ThrottleTime is only version 1 (0.9.0) and above
        ///  ThrottleTime => int32        -- Duration in milliseconds for which the request was throttled due to quota violation. (Zero if the request did not 
        ///                                  violate any quota.)
        ///  TopicName => string          -- The topic this response entry corresponds to.
        ///  Partition => int32           -- The partition this response entry corresponds to.
        ///  ErrorCode => int16           -- The error from this partition, if any. Errors are given on a per-partition basis because a given partition may 
        ///                                  be unavailable or maintained on a different host, while others may have successfully accepted the produce request.
        ///  HighwaterMarkOffset => int64 -- The offset at the end of the log for this partition. This can be used by the client to determine how many messages 
        ///                                  behind the end of the log they are.
        ///  MessageSetSize => int32      -- The size in bytes of the message set for this partition
        /// 
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-FetchResponse
        /// </summary>
        public static FetchResponse FetchResponse(short version, byte[] payload)
        {
            using (var stream = new BigEndianBinaryReader(payload)) {
                var correlationId = stream.ReadInt32();
                TimeSpan? throttleTime = null;

                if (version >= 1) {
                    throttleTime = TimeSpan.FromMilliseconds(stream.ReadInt32());
                }

                var topics = new List<FetchTopicResponse>();
                var topicCount = stream.ReadInt32();
                for (int t = 0; t < topicCount; t++) {
                    var topicName = stream.ReadInt16String();

                    var partitionCount = stream.ReadInt32();
                    for (int p = 0; p < partitionCount; p++) {
                        var partitionId = stream.ReadInt32();
                        var errorCode = (ErrorResponseCode) stream.ReadInt16();
                        var highWaterMarkOffset = stream.ReadInt64();
                        var messages = Message.DecodeMessageSet(stream.ReadIntPrefixedBytes())
                                               .Select(
                                                   x => {
                                                       x.Meta.PartitionId = partitionId;
                                                       return x;
                                                   })
                                               .ToList();
                        topics.Add(new FetchTopicResponse(topicName, partitionId, highWaterMarkOffset, errorCode, messages));
                    }
                }
                return new FetchResponse(correlationId, topics, throttleTime);
            }
        }

        /// <summary>
        /// OffsetResponse => [TopicName [Partition ErrorCode [Offset]]]
        ///  TopicName => string  -- The name of the topic.
        ///  Partition => int32   -- The id of the partition the fetch is for.
        ///  ErrorCode => int16   -- The error from this partition, if any. Errors are given on a per-partition basis because a given partition may 
        ///                          be unavailable or maintained on a different host, while others may have successfully accepted the produce request.
        ///  Offset => int64
        /// 
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetAPI(AKAListOffset)
        /// </summary>
        public static OffsetResponse OffsetResponse(short version, byte[] payload)
        {
            using (var stream = new BigEndianBinaryReader(payload)) {
                var correlationId = stream.ReadInt32();

                var topics = new List<OffsetTopic>();
                var topicCount = stream.ReadInt32();
                for (int t = 0; t < topicCount; t++) {
                    var topicName = stream.ReadInt16String();

                    var partitionCount = stream.ReadInt32();
                    for (int p = 0; p < partitionCount; p++) {
                        var partitionId = stream.ReadInt32();
                        var errorCode = (ErrorResponseCode) stream.ReadInt16();

                        var offsets = new List<long>();
                        var offsetCount = stream.ReadInt32();
                        for (int o = 0; o < offsetCount; o++) {
                            offsets.Add(stream.ReadInt64());
                        }

                        topics.Add(new OffsetTopic(topicName, partitionId, errorCode, offsets));
                    }
                }
                return new OffsetResponse(correlationId, topics);
            }
        }

        /// <summary>
        /// MetadataResponse => [Broker][TopicMetadata]
        ///  Broker => NodeId Host Port  (any number of brokers may be returned)
        ///                               -- The node id, hostname, and port information for a kafka broker
        ///   NodeId => int32             -- The broker id.
        ///   Host => string              -- The hostname of the broker.
        ///   Port => int32               -- The port on which the broker accepts requests.
        ///  TopicMetadata => TopicErrorCode TopicName [PartitionMetadata]
        ///   TopicErrorCode => int16     -- The error code for the given topic.
        ///   TopicName => string         -- The name of the topic.
        ///  PartitionMetadata => PartitionErrorCode PartitionId Leader Replicas Isr
        ///   PartitionErrorCode => int16 -- The error code for the partition, if any.
        ///   PartitionId => int32        -- The id of the partition.
        ///   Leader => int32             -- The id of the broker acting as leader for this partition.
        ///                                  If no leader exists because we are in the middle of a leader election this id will be -1.
        ///   Replicas => [int32]         -- The set of all nodes that host this partition.
        ///   Isr => [int32]              -- The set of nodes that are in sync with the leader for this partition.
        ///
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-MetadataAPI
        /// </summary>
        public static MetadataResponse MetadataResponse(short version, byte[] payload)
        {
            using (var stream = new BigEndianBinaryReader(payload)) {
                var correlationId = stream.ReadInt32();

                var brokers = BrokerMetadatas(version, stream);
                //var brokers = new List<Broker>();
                //var brokerCount = stream.ReadInt32();
                //for (var b = 0; b < brokerCount; b++) {
                //    var brokerId = stream.ReadInt32();
                //    var host = stream.ReadInt16String();
                //    var port = stream.ReadInt32();

                //    brokers.Add(new Broker(brokerId, host, port));
                //}

                var topics = TopicMetadatas(version, stream);
                //var topics = new List<Topic>();
                //var topicCount = stream.ReadInt32();
                //for (var t = 0; t < topicCount; t++) {
                //    var topicError = (ErrorResponseCode) stream.ReadInt16();
                //    var topicName = stream.ReadInt16String();

                //    var partitions = new List<Partition>();
                //    var partitionCount = stream.ReadInt32();
                //    for (var p = 0; p < partitionCount; p++) {
                //        var partitionError = (ErrorResponseCode) stream.ReadInt16();
                //        var partitionId = stream.ReadInt32();
                //        var leaderId = stream.ReadInt32();

                //        var replicaCount = stream.ReadInt32();
                //        var replicas = replicaCount.Repeat(stream.ReadInt32);

                //        var isrCount = stream.ReadInt32();
                //        var isrs = isrCount.Repeat(stream.ReadInt32);

                //        partitions.Add(new Partition(partitionError, partitionId, leaderId, replicas, isrs));

                //    }
                //    topics.Add(new Topic(topicError, topicName, partitions));
                //}

                return new MetadataResponse(correlationId, brokers, topics);
            }
        }

        /// <summary>
        /// MetadataResponse => [Broker] ...
        ///  Broker => NodeId Host Port  (any number of brokers may be returned)
        ///                               -- The node id, hostname, and port information for a kafka broker
        ///   NodeId => int32             -- The broker id.
        ///   Host => string              -- The hostname of the broker.
        ///   Port => int32               -- The port on which the broker accepts requests.
        /// </summary>
        private static IEnumerable<MetadataBroker> BrokerMetadatas(short version, BigEndianBinaryReader stream)
        {
            var brokerCount = stream.ReadInt32();
            for (var b = 0; b < brokerCount; b++) {
                var brokerId = stream.ReadInt32();
                var host = stream.ReadInt16String();
                var port = stream.ReadInt32();

                yield return new MetadataBroker(brokerId, host, port);
            }
        }

        /// <summary>
        /// MetadataResponse => ... [TopicMetadata]
        ///  TopicMetadata => TopicErrorCode TopicName [PartitionMetadata]
        ///   TopicErrorCode => int16     -- The error code for the given topic.
        ///   TopicName => string         -- The name of the topic.
        ///  PartitionMetadata => PartitionErrorCode PartitionId Leader Replicas Isr
        ///   PartitionErrorCode => int16 -- The error code for the partition, if any.
        ///   PartitionId => int32        -- The id of the partition.
        ///   Leader => int32             -- The id of the broker acting as leader for this partition.
        ///                                  If no leader exists because we are in the middle of a leader election this id will be -1.
        ///   Replicas => [int32]         -- The set of all nodes that host this partition.
        ///   Isr => [int32]              -- The set of nodes that are in sync with the leader for this partition.
        /// </summary>
        private static IEnumerable<MetadataTopic> TopicMetadatas(short version, BigEndianBinaryReader stream)
        {
            var topicCount = stream.ReadInt32();
            for (var t = 0; t < topicCount; t++) {
                var topicError = (ErrorResponseCode) stream.ReadInt16();
                var topicName = stream.ReadInt16String();

                var partitions = new List<MetadataPartition>();
                var partitionCount = stream.ReadInt32();
                for (var p = 0; p < partitionCount; p++) {
                    var partitionError = (ErrorResponseCode) stream.ReadInt16();
                    var partitionId = stream.ReadInt32();
                    var leaderId = stream.ReadInt32();

                    var replicaCount = stream.ReadInt32();
                    var replicas = replicaCount.Repeat(stream.ReadInt32);

                    var isrCount = stream.ReadInt32();
                    var isrs = isrCount.Repeat(stream.ReadInt32);

                    partitions.Add(new MetadataPartition(partitionId, leaderId, partitionError, replicas, isrs));

                }
                yield return new MetadataTopic(topicName, topicError, partitions);
            }
        }


        /// <summary>
        /// OffsetCommitResponse => [TopicName [Partition ErrorCode]]]
        ///  TopicName => string -- The name of the topic.
        ///  Partition => int32  -- The id of the partition.
        ///  ErrorCode => int16  -- The error code for the partition, if any.
        ///
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
        /// </summary>
        public static OffsetCommitResponse OffsetCommitResponse(short version, byte[] payload)
        {
            using (var stream = new BigEndianBinaryReader(payload)) {
                var correlationId = stream.ReadInt32();

                var topics = new List<TopicResponse>();
                var topicCount = stream.ReadInt32();
                for (int t = 0; t < topicCount; t++) {
                    var topicName = stream.ReadInt16String();

                    var partitionCount = stream.ReadInt32();
                    for (int p = 0; p < partitionCount; p++) {
                        var partitionId = stream.ReadInt32();
                        var errorCode = (ErrorResponseCode) stream.ReadInt16();

                        topics.Add(new TopicResponse(topicName, partitionId, errorCode));
                    }
                }

                return new OffsetCommitResponse(correlationId, topics);
            }
        }

        /// <summary>
        /// OffsetFetchResponse => [TopicName [Partition Offset Metadata ErrorCode]]
        ///  TopicName => string -- The name of the topic.
        ///  Partition => int32  -- The id of the partition.
        ///  Offset => int64     -- The offset, or -1 if none exists.
        ///  Metadata => string  -- The metadata associated with the topic and partition.
        ///  ErrorCode => int16  -- The error code for the partition, if any.
        ///
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
        /// </summary>
        public static OffsetFetchResponse OffsetFetchResponse(short version, byte[] payload)
        {
            using (var stream = new BigEndianBinaryReader(payload)) {
                var correlationId = stream.ReadInt32();

                var topics = new List<OffsetFetchTopic>();
                var topicCount = stream.ReadInt32();
                for (int t = 0; t < topicCount; t++) {
                    var topicName = stream.ReadInt16String();

                    var partitionCount = stream.ReadInt32();
                    for (int p = 0; p < partitionCount; p++) {
                        var partitionId = stream.ReadInt32();
                        var offset = stream.ReadInt64();
                        var metadata = stream.ReadInt16String();
                        var errorCode = (ErrorResponseCode) stream.ReadInt16();

                        topics.Add(new OffsetFetchTopic(topicName, partitionId, errorCode, offset, metadata));
                    }
                }

                return new OffsetFetchResponse(correlationId, topics);
            }
        }
        
        /// <summary>
        /// GroupCoordinatorResponse => ErrorCode CoordinatorId CoordinatorHost CoordinatorPort
        ///  ErrorCode => int16        -- The error code.
        ///  CoordinatorId => int32    -- The broker id.
        ///  CoordinatorHost => string -- The hostname of the broker.
        ///  CoordinatorPort => int32  -- The port on which the broker accepts requests.
        ///
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
        /// </summary>
        public static GroupCoordinatorResponse GroupCoordinatorResponse(short version, byte[] payload)
        {
            using (var stream = new BigEndianBinaryReader(payload)) {
                var correlationId = stream.ReadInt32();
                var errorCode = (ErrorResponseCode)stream.ReadInt16();
                var coordinatorId = stream.ReadInt32();
                var coordinatorHost = stream.ReadInt16String();
                var coordinatorPort = stream.ReadInt32();

                return new GroupCoordinatorResponse(correlationId, errorCode, coordinatorId, coordinatorHost, coordinatorPort);
            }
        }

        /// <summary>
        /// ApiVersionsResponse => ErrorCode [ApiKey MinVersion MaxVersion]
        ///  ErrorCode => int16  -- The error code.
        ///  ApiKey => int16     -- The Api Key.
        ///  MinVersion => int16 -- The minimum supported version.
        ///  MaxVersion => int16 -- The maximum supported version.
        ///
        /// From http://kafka.apache.org/protocol.html#protocol_messages
        /// </summary>
        public static ApiVersionsResponse ApiVersionsResponse(short version, byte[] payload)
        {
            using (var stream = new BigEndianBinaryReader(payload)) {
                var correlationId = stream.ReadInt32();
                var errorCode = (ErrorResponseCode)stream.ReadInt16();

                var apiKeys = new ApiVersionSupport[stream.ReadInt32()];
                for (var i = 0; i < apiKeys.Length; i++) {
                    var apiKey = (ApiKeyRequestType)stream.ReadInt16();
                    var minVersion = stream.ReadInt16();
                    var maxVersion = stream.ReadInt16();
                    apiKeys[i] = new ApiVersionSupport(apiKey, minVersion, maxVersion);
                }
                return new ApiVersionsResponse(correlationId, errorCode, apiKeys);
            }
        }
    }
}