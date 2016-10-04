using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using KafkaClient.Common;
using KafkaClient.Protocol;

namespace KafkaClient.Tests.Unit
{
    public static class KafkaDecoder
    {
        public static IRequestContext DecodeHeader(byte[] data)
        {
            IRequestContext context;
            using (ReadHeader(data, out context)) {
                return context;
            }            
        }

        public static T Decode<T>(byte[] payload) where T : class, IRequest
        {
            IRequestContext context;
            using (ReadHeader(payload, out context)) { }

            return Decode<T>(context, payload);
        }

        public static T Decode<T>(IRequestContext context, byte[] payload) where T : class, IRequest
        {
            if (typeof(T) == typeof(FetchRequest)) return (T)FetchRequest(context, payload);
            if (typeof(T) == typeof(MetadataRequest)) return (T)MetadataRequest(context, payload);
            if (typeof(T) == typeof(ProduceRequest)) return (T)ProduceRequest(context, payload);
            if (typeof(T) == typeof(OffsetRequest)) return (T)OffsetRequest(context, payload);
            if (typeof(T) == typeof(OffsetCommitRequest)) return (T)OffsetCommitRequest(context, payload);
            if (typeof(T) == typeof(OffsetFetchRequest)) return (T)OffsetFetchRequest(context, payload);
            if (typeof(T) == typeof(GroupCoordinatorRequest)) return (T)GroupCoordinatorRequest(context, payload);
            if (typeof(T) == typeof(ApiVersionsRequest)) return (T)ApiVersionsRequest(context, payload);
            return default(T);
        }

        public static byte[] EncodeResponseBytes<T>(IRequestContext context, T response) where T : IResponse
        {
            byte[] data = null;
            using (var stream = new MemoryStream()) {
                var writer = new BigEndianBinaryWriter(stream);
                writer.Write(0); // size placeholder
                // From http://kafka.apache.org/protocol.html#protocol_messages
                // 
                // Response Header => correlation_id 
                //  correlation_id => INT32  -- The user-supplied value passed in with the request
                writer.Write(context.CorrelationId);

                var isEncoded = TryEncodeResponse(writer, context, response as FetchResponse)
                || TryEncodeResponse(writer, context, response as MetadataResponse)
                || TryEncodeResponse(writer, context, response as ProduceResponse)
                || TryEncodeResponse(writer, context, response as OffsetResponse)
                || TryEncodeResponse(writer, context, response as OffsetCommitResponse)
                || TryEncodeResponse(writer, context, response as OffsetFetchResponse)
                || TryEncodeResponse(writer, context, response as GroupCoordinatorResponse)
                || TryEncodeResponse(writer, context, response as ApiVersionsResponse);

                data = new byte[stream.Position];
                stream.Position = 0;
                writer.Write(data.Length - 4);
                Buffer.BlockCopy(stream.GetBuffer(), 0, data, 0, data.Length);
            }
            return data;
        }

        #region Decode

        /// <summary>
        /// ProduceRequest => RequiredAcks Timeout [TopicName [Partition MessageSetSize MessageSet]]
        ///  RequiredAcks => int16   -- This field indicates how many acknowledgements the servers should receive before responding to the request. 
        ///                             If it is 0 the server will not send any response (this is the only case where the server will not reply to 
        ///                             a request). If it is 1, the server will wait the data is written to the local log before sending a response. 
        ///                             If it is -1 the server will block until the message is committed by all in sync replicas before sending a response.
        ///  Timeout => int32        -- This provides a maximum time in milliseconds the server can await the receipt of the number of acknowledgements 
        ///                             in RequiredAcks. The timeout is not an exact limit on the request time for a few reasons: (1) it does not include 
        ///                             network latency, (2) the timer begins at the beginning of the processing of this request so if many requests are 
        ///                             queued due to server overload that wait time will not be included, (3) we will not terminate a local write so if 
        ///                             the local write time exceeds this timeout it will not be respected. To get a hard timeout of this type the client 
        ///                             should use the socket timeout.
        ///  TopicName => string     -- The topic that data is being published to.
        ///  Partition => int32      -- The partition that data is being published to.
        ///  MessageSetSize => int32 -- The size, in bytes, of the message set that follows.
        /// 
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets
        /// </summary>
        private static IRequest ProduceRequest(IRequestContext context, byte[] data)
        {
            using (var stream = ReadHeader(data)) {
                var acks = stream.ReadInt16();
                var timeout = stream.ReadInt32();

                var payloads = new List<Payload>();
                var payloadCount = stream.ReadInt32();
                for (var i = 0; i < payloadCount; i++) {
                    var topicName = stream.ReadInt16String();

                    var partitionCount = stream.ReadInt32();
                    for (var j = 0; j < partitionCount; j++) {
                        var partitionId = stream.ReadInt32();
                        var messages = KafkaEncoder.DecodeMessageSet(stream.ReadIntPrefixedBytes(), partitionId).ToList();

                        payloads.Add(new Payload(topicName, partitionId, messages));
                    }
                }
                return new ProduceRequest(payloads, timeout > 0 ? TimeSpan.FromMilliseconds(timeout) : (TimeSpan?)null, acks);
            }
        }

        /// <summary>
        /// FetchRequest => ReplicaId MaxWaitTime MinBytes [TopicName [Partition FetchOffset MaxBytes]]
        ///  ReplicaId => int32   -- The replica id indicates the node id of the replica initiating this request. Normal client consumers should always 
        ///                          specify this as -1 as they have no node id. Other brokers set this to be their own node id. The value -2 is accepted 
        ///                          to allow a non-broker to issue fetch requests as if it were a replica broker for debugging purposes.
        ///  MaxWaitTime => int32 -- The max wait time is the maximum amount of time in milliseconds to block waiting if insufficient data is available 
        ///                          at the time the request is issued.
        ///  MinBytes => int32    -- This is the minimum number of bytes of messages that must be available to give a response. If the client sets this 
        ///                          to 0 the server will always respond immediately, however if there is no new data since their last request they will 
        ///                          just get back empty message sets. If this is set to 1, the server will respond as soon as at least one partition has 
        ///                          at least 1 byte of data or the specified timeout occurs. By setting higher values in combination with the timeout the 
        ///                          consumer can tune for throughput and trade a little additional latency for reading only large chunks of data (e.g. 
        ///                          setting MaxWaitTime to 100 ms and setting MinBytes to 64k would allow the server to wait up to 100ms to try to accumulate 
        ///                          64k of data before responding).
        ///  TopicName => string  -- The name of the topic.
        ///  Partition => int32   -- The id of the partition the fetch is for.
        ///  FetchOffset => int64 -- The offset to begin this fetch from.
        ///  MaxBytes => int32    -- The maximum bytes to include in the message set for this partition. This helps bound the size of the response.
        /// 
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-FetchAPI
        /// </summary>
        private static IRequest FetchRequest(IRequestContext context, byte[] data)
        {
            using (var stream = ReadHeader(data)) {
                var replicaId = stream.ReadInt32(); // expect -1
                var maxWaitTime = stream.ReadInt32();
                var minBytes = stream.ReadInt32();

                var fetches = new List<Fetch>();
                var payloadCount = stream.ReadInt32();
                for (var i = 0; i < payloadCount; i++) {
                    var topicName = stream.ReadInt16String();

                    var partitionCount = stream.ReadInt32();
                    for (var j = 0; j < partitionCount; j++) {
                        var partitionId = stream.ReadInt32();
                        var offset = stream.ReadInt64();
                        var maxBytes = stream.ReadInt32();

                        fetches.Add(new Fetch(topicName, partitionId, offset, maxBytes));
                    }
                }
                return new FetchRequest(fetches, maxWaitTime > 0 ? TimeSpan.FromMilliseconds(maxWaitTime) : (TimeSpan?)null, minBytes);
            }
        }

        /// <summary>
        /// OffsetRequest => ReplicaId [TopicName [Partition Time MaxNumberOfOffsets]]
        ///  ReplicaId => int32   -- The replica id indicates the node id of the replica initiating this request. Normal client consumers should always 
        ///                          specify this as -1 as they have no node id. Other brokers set this to be their own node id. The value -2 is accepted 
        ///                          to allow a non-broker to issue fetch requests as if it were a replica broker for debugging purposes.
        ///  TopicName => string  -- The name of the topic.
        ///  Partition => int32   -- The id of the partition the fetch is for.
        ///  Time => int64        -- Used to ask for all messages before a certain time (ms). There are two special values. Specify -1 to receive the 
        ///                          latest offset (i.e. the offset of the next coming message) and -2 to receive the earliest available offset. Note 
        ///                          that because offsets are pulled in descending order, asking for the earliest offset will always return you a single element.
        ///  MaxNumberOfOffsets => int32 
        /// 
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetAPI(AKAListOffset)
        /// </summary>
        private static IRequest OffsetRequest(IRequestContext context, byte[] data)
        {
            using (var stream = ReadHeader(data)) {
                var replicaId = stream.ReadInt32(); // expect -1

                var offsets = new List<Offset>();
                var offsetCount = stream.ReadInt32();
                for (var i = 0; i < offsetCount; i++) {
                    var topicName = stream.ReadInt16String();

                    var partitionCount = stream.ReadInt32();
                    for (var j = 0; j < partitionCount; j++) {
                        var partitionId = stream.ReadInt32();
                        var time = stream.ReadInt64();
                        var maxOffsets = stream.ReadInt32();

                        offsets.Add(new Offset(topicName, partitionId, time, maxOffsets));
                    }
                }
                return new OffsetRequest(offsets);
            }
        }

        /// <summary>
        /// TopicMetadataRequest => [TopicName]
        ///  TopicName => string  -- The topics to produce metadata for. If empty the request will yield metadata for all topics.
        ///
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-MetadataAPI
        /// </summary>
        private static IRequest MetadataRequest(IRequestContext context, byte[] data)
        {
            using (var stream = ReadHeader(data)) {
                var topicNames = new List<string>();
                var count = stream.ReadInt32();
                for (var t = 0; t < count; t++) {
                    var topicName = stream.ReadInt16String();
                    topicNames.Add(topicName);
                }

                return new MetadataRequest(topicNames);
            }
        }
        
        /// <summary>
        /// OffsetCommitRequest => ConsumerGroup *ConsumerGroupGenerationId *MemberId *RetentionTime [TopicName [Partition Offset *TimeStamp Metadata]]
        /// *ConsumerGroupGenerationId, MemberId is only version 1 (0.8.2) and above
        /// *TimeStamp is only version 1 (0.8.2)
        /// *RetentionTime is only version 2 (0.9.0) and above
        ///  ConsumerGroupId => string          -- The consumer group id.
        ///  ConsumerGroupGenerationId => int32 -- The generation of the consumer group.
        ///  MemberId => string                 -- The consumer id assigned by the group coordinator.
        ///  RetentionTime => int64             -- Time period in ms to retain the offset.
        ///  TopicName => string                -- The topic to commit.
        ///  Partition => int32                 -- The partition id.
        ///  Offset => int64                    -- message offset to be committed.
        ///  Timestamp => int64                 -- Commit timestamp.
        ///  Metadata => string                 -- Any associated metadata the client wants to keep
        ///
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
        /// </summary>
        private static IRequest OffsetCommitRequest(IRequestContext context, byte[] payload)
        {
            using (var stream = ReadHeader(payload)) {
                var consumerGroup = stream.ReadInt16String();
                var generationId = 0;
                string memberId = null; 
                if (context.ApiVersion >= 1) {
                    generationId = stream.ReadInt32();
                    memberId = stream.ReadInt16String();
                }
                TimeSpan? offsetRetention = null;
                if (context.ApiVersion >= 2) {
                    var retentionTime = stream.ReadInt64();
                    if (retentionTime >= 0) {
                        offsetRetention = TimeSpan.FromMilliseconds(retentionTime);
                    }
                }

                var offsetCommits = new List<OffsetCommit>();
                var count = stream.ReadInt32();
                for (var o = 0; o < count; o++) {
                    var topicName = stream.ReadInt16String();

                    var partitionCount = stream.ReadInt32();
                    for (var p = 0; p < partitionCount; p++) {
                        var partitionId = stream.ReadInt32();
                        var offset = stream.ReadInt64();
                        long? timestamp = null;
                        if (context.ApiVersion == 1) {
                            timestamp = stream.ReadInt64();
                        }
                        var metadata = stream.ReadInt16String();

                        offsetCommits.Add(new OffsetCommit(topicName, partitionId, offset, metadata, timestamp));
                    }
                }

                return new OffsetCommitRequest(consumerGroup, offsetCommits, memberId, generationId, offsetRetention);
            }
        }

        /// <summary>
        /// OffsetFetchRequest => ConsumerGroup [TopicName [Partition]]
        ///  ConsumerGroup => string -- The consumer group id.
        ///  TopicName => string     -- The topic to commit.
        ///  Partition => int32      -- The partition id.
        ///
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
        /// </summary>
        private static IRequest OffsetFetchRequest(IRequestContext context, byte[] payload)
        {
            using (var stream = ReadHeader(payload)) {
                var consumerGroup = stream.ReadInt16String();

                var topics = new List<Topic>();
                var count = stream.ReadInt32();
                for (var t = 0; t < count; t++) {
                    var topicName = stream.ReadInt16String();

                    var partitionCount = stream.ReadInt32();
                    for (var p = 0; p < partitionCount; p++) {
                        var partitionId = stream.ReadInt32();

                        topics.Add(new Topic(topicName, partitionId));
                    }
                }

                return new OffsetFetchRequest(consumerGroup, topics);
            }
        }
        
        /// <summary>
        /// GroupCoordinatorRequest => GroupId
        ///  GroupId => string -- The consumer group id.
        ///
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
        /// </summary>
        private static IRequest GroupCoordinatorRequest(IRequestContext context, byte[] payload)
        {
            using (var stream = ReadHeader(payload)) {
                var groupId = stream.ReadInt16String();

                return new GroupCoordinatorRequest(groupId);
            }
        }

        /// <summary>
        /// ApiVersionsRequest => 
        ///
        /// From http://kafka.apache.org/protocol.html#protocol_messages
        /// </summary>
        private static IRequest ApiVersionsRequest(IRequestContext context, byte[] payload)
        {
            using (var stream = ReadHeader(payload)) {
                return new ApiVersionsRequest();
            }
        }
        
        private static BigEndianBinaryReader ReadHeader(byte[] data)
        {
            IRequestContext context;
            return ReadHeader(data, out context);
        }

        private static BigEndianBinaryReader ReadHeader(byte[] data, out IRequestContext context)
        {
            var stream = new BigEndianBinaryReader(data, 4);
            try {
                var apiKey = (ApiKeyRequestType) stream.ReadInt16();
                var version = stream.ReadInt16();
                var correlationId = stream.ReadInt32();
                var clientId = stream.ReadInt16String();

                context = new RequestContext(correlationId, version, clientId);
            } catch {
                context = null;
                stream?.Dispose();
                stream = null;
            }
            return stream;
        } 

        #endregion

        #region Encode

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
        private static bool TryEncodeResponse(BigEndianBinaryWriter writer, IRequestContext context, ProduceResponse response)
        {
            if (response == null) return false;

            var groupedTopics = response.Topics.GroupBy(t => t.TopicName).ToList();
            writer.Write(groupedTopics.Count);
            foreach (var topic in groupedTopics) {
                writer.Write(topic.Key, StringPrefixEncoding.Int16);
                var partitions = topic.ToList();

                writer.Write(partitions.Count); // partitionsPerTopic
                foreach (var partition in partitions) {
                    writer.Write(partition.PartitionId);
                    writer.Write((short)partition.ErrorCode);
                    writer.Write(partition.Offset);
                    if (context.ApiVersion >= 2) {
                        writer.Write(partition.Timestamp?.ToUnixEpochMilliseconds() ?? -1L);
                    }
                }
            }
            if (context.ApiVersion >= 1) {
                writer.Write((int?)response.ThrottleTime?.TotalMilliseconds ?? 0);
            }
            return true;
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
        private static bool TryEncodeResponse(BigEndianBinaryWriter writer, IRequestContext context, FetchResponse response)
        {
            if (response == null) return false;

            if (context.ApiVersion >= 1) {
                writer.Write((int?)response.ThrottleTime?.TotalMilliseconds ?? 0);
            }
            var groupedTopics = response.Topics.GroupBy(t => t.TopicName).ToList();
            writer.Write(groupedTopics.Count);
            foreach (var topic in groupedTopics) {
                writer.Write(topic.Key, StringPrefixEncoding.Int16);
                var partitions = topic.ToList();

                writer.Write(partitions.Count); // partitionsPerTopic
                foreach (var partition in partitions) {
                    writer.Write(partition.PartitionId);
                    writer.Write((short)partition.ErrorCode);
                    writer.Write(partition.HighWaterMark);

                    var messageSet = KafkaEncoder.EncodeMessageSet(partition.Messages);
                    writer.Write(messageSet.Length);
                    writer.Write(messageSet);
                }
            }
            return true;
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
        private static bool TryEncodeResponse(BigEndianBinaryWriter writer, IRequestContext context, OffsetResponse response)
        {
            if (response == null) return false;

            var groupedTopics = response.Topics.GroupBy(t => t.TopicName).ToList();
            writer.Write(groupedTopics.Count);
            foreach (var topic in groupedTopics) {
                writer.Write(topic.Key, StringPrefixEncoding.Int16);
                var partitions = topic.ToList();

                writer.Write(partitions.Count); // partitionsPerTopic
                foreach (var partition in partitions) {
                    writer.Write(partition.PartitionId);
                    writer.Write((short)partition.ErrorCode);
                    writer.Write(partition.Offsets.Count);
                    foreach (var offset in partition.Offsets) {
                        writer.Write(offset);
                    }
                }
            }
            return true;
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
        private static bool TryEncodeResponse(BigEndianBinaryWriter writer, IRequestContext context, MetadataResponse response)
        {
            if (response == null) return false;

            writer.Write(response.Brokers.Count);
            foreach (var broker in response.Brokers) {
                writer.Write(broker.BrokerId);
                writer.Write(broker.Host, StringPrefixEncoding.Int16);
                writer.Write(broker.Port);
            }

            var groupedTopics = response.Topics.GroupBy(t => new { t.TopicName, t.ErrorCode }).ToList();
            writer.Write(groupedTopics.Count);
            foreach (var topic in groupedTopics) {
                writer.Write((short)topic.Key.ErrorCode);
                writer.Write(topic.Key.TopicName, StringPrefixEncoding.Int16);
                var partitions = topic.SelectMany(_ => _.Partitions).ToList();
                writer.Write(partitions.Count); // partitionsPerTopic
                foreach (var partition in partitions) {
                    writer.Write((short)partition.ErrorCode);
                    writer.Write(partition.PartitionId);
                    writer.Write(partition.LeaderId);
                    writer.Write(partition.Replicas.Count);
                    foreach (var replica in partition.Replicas) {
                        writer.Write(replica);
                    }
                    writer.Write(partition.Isrs.Count);
                    foreach (var isr in partition.Isrs) {
                        writer.Write(isr);
                    }
                }
            }
            return true;
        }
        
        /// <summary>
        /// OffsetCommitResponse => [TopicName [Partition ErrorCode]]]
        ///  TopicName => string -- The name of the topic.
        ///  Partition => int32  -- The id of the partition.
        ///  ErrorCode => int16  -- The error code for the partition, if any.
        ///
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
        /// </summary>
        private static bool TryEncodeResponse(BigEndianBinaryWriter writer, IRequestContext context, OffsetCommitResponse response)
        {
            if (response == null) return false;

            var groupedTopics = response.Topics.GroupBy(t => t.TopicName).ToList();
            writer.Write(groupedTopics.Count);
            foreach (var topic in groupedTopics) {
                writer.Write(topic.Key, StringPrefixEncoding.Int16);
                var partitions = topic.ToList();
                writer.Write(partitions.Count); // partitionsPerTopic
                foreach (var partition in partitions) {
                    writer.Write(partition.PartitionId);
                    writer.Write((short)partition.ErrorCode);
                }
            }
            return true;
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
        private static bool TryEncodeResponse(BigEndianBinaryWriter writer, IRequestContext context, OffsetFetchResponse response)
        {
            if (response == null) return false;

            var groupedTopics = response.Topics.GroupBy(t => t.TopicName).ToList();
            writer.Write(groupedTopics.Count);
            foreach (var topic in groupedTopics) {
                writer.Write(topic.Key, StringPrefixEncoding.Int16);
                var partitions = topic.ToList();
                writer.Write(partitions.Count); // partitionsPerTopic
                foreach (var partition in partitions) {
                    writer.Write(partition.PartitionId);
                    writer.Write(partition.Offset);
                    writer.Write(partition.MetaData, StringPrefixEncoding.Int16);
                    writer.Write((short)partition.ErrorCode);
                }
            }
            return true;
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
        private static bool TryEncodeResponse(BigEndianBinaryWriter writer, IRequestContext context, GroupCoordinatorResponse response)
        {
            if (response == null) return false;

            writer.Write((short)response.ErrorCode);
            writer.Write(response.BrokerId);
            writer.Write(response.Host, StringPrefixEncoding.Int16);
            writer.Write(response.Port);
            return true;
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
        private static bool TryEncodeResponse(BigEndianBinaryWriter writer, IRequestContext context, ApiVersionsResponse response)
        {
            if (response == null) return false;

            writer.Write((short)response.ErrorCode);
            writer.Write(response.SupportedVersions.Count); // partitionsPerTopic
            foreach (var versionSupport in response.SupportedVersions) {
                writer.Write((short)versionSupport.ApiKey);
                writer.Write(versionSupport.MinVersion);
                writer.Write(versionSupport.MaxVersion);
            }
            return true;
        }

        #endregion

        public static byte[] PrefixWithInt32Length(this byte[] source)
        {
            var destination = new byte[source.Length + 4]; 
            using (var stream = new MemoryStream(destination)) {
                using (var writer = new BigEndianBinaryWriter(stream)) {
                    writer.Write(source.Length);
                }
            }
            Buffer.BlockCopy(source, 0, destination, 4, source.Length);
            return destination;
        }
    }
}