using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using KafkaNet.Common;
using KafkaNet.Statistics;

namespace KafkaNet.Protocol
{
    public static class EncodeRequest
    {
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
        public static byte[] ProduceRequest(ProduceRequest request)
        {
            int totalCompressedBytes = 0;
            if (request.Payload == null) request.Payload = new List<Payload>();

            var groupedPayloads = (from p in request.Payload
                                   group p by new
                                   {
                                       p.Topic,
                                       p.Partition,
                                       p.Codec
                                   } into tpc
                                   select tpc).ToList();

            using (var message = RequestHeader(request)
                .Pack(request.Acks)
                .Pack(request.TimeoutMS)
                .Pack(groupedPayloads.Count))
            {
                foreach (var groupedPayload in groupedPayloads) {
                    var payloads = groupedPayload.ToList();
                    message.Pack(groupedPayload.Key.Topic, StringPrefixEncoding.Int16)
                        .Pack(payloads.Count)
                        .Pack(groupedPayload.Key.Partition);

                    switch (groupedPayload.Key.Codec)
                    {
                        case MessageCodec.CodecNone:
                            message.Pack(Message.EncodeMessageSet(payloads.SelectMany(x => x.Messages)));
                            break;

                        case MessageCodec.CodecGzip:
                            var compressedBytes = CreateGzipCompressedMessage(payloads.SelectMany(x => x.Messages));
                            Interlocked.Add(ref totalCompressedBytes, compressedBytes.CompressedAmount);
                            message.Pack(Message.EncodeMessageSet(new[] { compressedBytes.CompressedMessage }));
                            break;

                        default:
                            throw new NotSupportedException($"Codec type of {groupedPayload.Key.Codec} is not supported.");
                    }
                }

                var bytes = message.Payload();
                StatisticsTracker.RecordProduceRequest(request.Payload.Sum(x => x.Messages.Count), bytes.Length, totalCompressedBytes);
                return bytes;
            }
        }

        internal class CompressedMessageResult
        {
            public int CompressedAmount { get; set; }
            public Message CompressedMessage { get; set; }
        }

        private static CompressedMessageResult CreateGzipCompressedMessage(IEnumerable<Message> messages)
        {
            var messageSet = Message.EncodeMessageSet(messages);
            var gZipBytes = Compression.Zip(messageSet);

            var compressedMessage = new Message {
                Attribute = (byte)(0x00 | (Message.AttributeCodeMask & (byte)MessageCodec.CodecGzip)),
                Value = gZipBytes
            };

            return new CompressedMessageResult {
                CompressedAmount = messageSet.Length - compressedMessage.Value.Length,
                CompressedMessage = compressedMessage
            };
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
        public static byte[] FetchRequest(FetchRequest request)
        {
            using (var message = RequestHeader(request)) {
                var topicGroups = request.Fetches.GroupBy(x => x.Topic).ToList();
                message.Pack(ReplicaId)
                    .Pack(request.MaxWaitTime)
                    .Pack(request.MinBytes)
                    .Pack(topicGroups.Count);

                foreach (var topicGroup in topicGroups) {
                    var partitions = topicGroup.GroupBy(x => x.PartitionId).ToList();
                    message.Pack(topicGroup.Key, StringPrefixEncoding.Int16)
                        .Pack(partitions.Count);

                    foreach (var partition in partitions) {
                        foreach (var fetch in partition) {
                            message.Pack(partition.Key)
                                .Pack(fetch.Offset)
                                .Pack(fetch.MaxBytes);
                        }
                    }
                }

                return message.Payload();
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
        public static byte[] OffsetRequest(OffsetRequest request)
        {
            using (var message = RequestHeader(request)) {
                var topicGroups = request.Offsets.GroupBy(x => x.Topic).ToList();
                message.Pack(ReplicaId)
                    .Pack(topicGroups.Count);

                foreach (var topicGroup in topicGroups) {
                    var partitions = topicGroup.GroupBy(x => x.PartitionId).ToList();
                    message.Pack(topicGroup.Key, StringPrefixEncoding.Int16)
                        .Pack(partitions.Count);

                    foreach (var partition in partitions) {
                        foreach (var offset in partition) {
                            message.Pack(partition.Key)
                                .Pack(offset.Time)
                                .Pack(offset.MaxOffsets);
                        }
                    }
                }

                return message.Payload();
            }
        }

        /// <summary>
        /// TopicMetadataRequest => [TopicName]
        ///  TopicName => string  -- The topics to produce metadata for. If no topics are specified fetch metadata for all topics.
        ///
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-MetadataAPI
        /// </summary>
        public static byte[] MetadataRequest(MetadataRequest request)
        {
            using (var message = RequestHeader(request)) {
                message.Pack(request.Topics.Count)
                     .Pack(request.Topics, StringPrefixEncoding.Int16);

                return message.Payload();
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
        public static byte[] OffsetCommitRequest(OffsetCommitRequest request)
        {
            using (var message = RequestHeader(request)) {
                message.Pack(request.ConsumerGroup, StringPrefixEncoding.Int16);
                if (request.ApiVersion >= 1) {
                    message.Pack(request.GenerationId)
                            .Pack(request.MemberId, StringPrefixEncoding.Int16);
                }
                if (request.ApiVersion >= 2) {
                    if (request.OffsetRetention.HasValue) {
                        message.Pack((long) request.OffsetRetention.Value.TotalMilliseconds);
                    } else {
                        message.Pack(-1L);
                    }
                }

                var topicGroups = request.OffsetCommits.GroupBy(x => x.Topic).ToList();
                message.Pack(topicGroups.Count);

                foreach (var topicGroup in topicGroups) {
                    var partitions = topicGroup.GroupBy(x => x.PartitionId).ToList();
                    message.Pack(topicGroup.Key, StringPrefixEncoding.Int16)
                        .Pack(partitions.Count);

                    foreach (var partition in partitions) {
                        foreach (var commit in partition) {
                            message.Pack(partition.Key)
                            .Pack(commit.Offset);
                            if (request.ApiVersion == 1) {
                                message.Pack(commit.TimeStamp);
                            }
                            message.Pack(commit.Metadata, StringPrefixEncoding.Int16);
                        }
                    }
                }
                return message.Payload();
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
        public static byte[] OffsetFetchRequest(OffsetFetchRequest request)
        {
            using (var message = RequestHeader(request)) {
                var topicGroups = request.Topics.GroupBy(x => x.TopicName).ToList();

                message.Pack(request.ConsumerGroup, StringPrefixEncoding.Int16)
                    .Pack(topicGroups.Count);

                foreach (var topicGroup in topicGroups) {
                    var partitions = topicGroup.GroupBy(x => x.PartitionId).ToList();
                    message.Pack(topicGroup.Key, StringPrefixEncoding.Int16)
                        .Pack(partitions.Count);

                    foreach (var partition in partitions) {
                        foreach (var offset in partition) {
                            message.Pack(offset.PartitionId);
                        }
                    }
                }

                return message.Payload();
            }
        }

        /// <summary>
        /// GroupCoordinatorRequest => GroupId
        ///  GroupId => string -- The consumer group id.
        ///
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
        /// </summary>
        public static byte[] GroupCoordinatorRequest(GroupCoordinatorRequest request)
        {
            using (var message = RequestHeader(request)) {
                message.Pack(request.ConsumerGroup, StringPrefixEncoding.Int16);
                return message.Payload();
            }
        }

        /// <summary>
        /// ApiVersions => 
        ///
        /// From http://kafka.apache.org/protocol.html#protocol_messages
        /// </summary>
        public static byte[] ApiVersionsRequest(ApiVersionsRequest request)
        {
            using (var message = RequestHeader(request)) {
                return message.Payload();
            }
        }

        /// <summary>
        /// Encode the common head for kafka request.
        /// </summary>
        /// <remarks>
        /// Request Header => api_key api_version correlation_id client_id 
        ///  api_key => INT16             -- The id of the request type.
        ///  api_version => INT16         -- The version of the API.
        ///  correlation_id => INT32      -- A user-supplied integer value that will be passed back with the response.
        ///  client_id => NULLABLE_STRING -- A user specified identifier for the client making the request.
        /// </remarks>
        public static KafkaMessagePacker RequestHeader<T>(IKafkaRequest<T> request)
            where T : IKafkaResponse
        {
            return new KafkaMessagePacker()
                .Pack((short)request.ApiKey)
                 .Pack(request.ApiVersion)
                 .Pack(request.CorrelationId)
                 .Pack(request.ClientId, StringPrefixEncoding.Int16);
        }

        /// <summary>
        /// From Documentation:
        /// The replica id indicates the node id of the replica initiating this request. Normal client consumers should always specify this as -1 as they have no node id.
        /// Other brokers set this to be their own node id. The value -2 is accepted to allow a non-broker to issue fetch requests as if it were a replica broker for debugging purposes.
        ///
        /// Kafka Protocol implementation:
        /// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
        /// </summary>
        private const int ReplicaId = -1;
    }
}