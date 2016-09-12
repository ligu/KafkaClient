using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    public static class KafkaEncoder
    {
        public static T Decode<T>(IRequestContext context, byte[] payload) where T : class, IKafkaResponse
        {
            if (typeof(T) == typeof(FetchResponse)) return (T)FetchResponse(context, payload);
            if (typeof(T) == typeof(MetadataResponse)) return (T)MetadataResponse(context, payload);
            if (typeof(T) == typeof(ProduceResponse)) return (T)ProduceResponse(context, payload);
            if (typeof(T) == typeof(OffsetResponse)) return (T)OffsetResponse(context, payload);
            if (typeof(T) == typeof(OffsetCommitResponse)) return (T)OffsetCommitResponse(context, payload);
            if (typeof(T) == typeof(OffsetFetchResponse)) return (T)OffsetFetchResponse(context, payload);
            if (typeof(T) == typeof(GroupCoordinatorResponse)) return (T)GroupCoordinatorResponse(context, payload);
            if (typeof(T) == typeof(ApiVersionsResponse)) return (T)ApiVersionsResponse(context, payload);
            return default(T);
        }

        public static KafkaDataPayload Encode<T>(IRequestContext context, T request) where T : class, IKafkaRequest
        {
            switch (request.ApiKey) {
                case ApiKeyRequestType.Produce: {
                    var produceRequest = (ProduceRequest)(IKafkaRequest)request;
                    return new KafkaDataPayload {
                        Buffer = EncodeRequest(context, produceRequest),
                        CorrelationId = context.CorrelationId,
                        MessageCount = produceRequest.Payload.Sum(x => x.Messages.Count),
                        ApiKey = request.ApiKey
                    };
                }

                default:
                    return WrapEncodedBytes(context, request, EncodeRequestBytes(context, request));
            }
        }

        #region Encode

        private static KafkaDataPayload WrapEncodedBytes(IRequestContext context, IKafkaRequest request, byte[] buffer)
        {
            return new KafkaDataPayload {
                Buffer = buffer,
                CorrelationId = context.CorrelationId,
                ApiKey = request.ApiKey
            };
        }

        public static byte[] EncodeRequestBytes(IRequestContext context, IKafkaRequest request)
        {
            switch (request.ApiKey) {
                case ApiKeyRequestType.Fetch:
                    return EncodeRequest(context, (FetchRequest) request);
                case ApiKeyRequestType.Produce:
                    return EncodeRequest(context, (ProduceRequest) request);
                case ApiKeyRequestType.Metadata:
                    return EncodeRequest(context, (MetadataRequest) request);
                case ApiKeyRequestType.Offset:
                    return EncodeRequest(context, (OffsetRequest) request);
                case ApiKeyRequestType.OffsetFetch:
                    return EncodeRequest(context, (OffsetFetchRequest) request);
                case ApiKeyRequestType.OffsetCommit:
                    return EncodeRequest(context, (OffsetCommitRequest) request);
                case ApiKeyRequestType.GroupCoordinator:
                    return EncodeRequest(context, (GroupCoordinatorRequest) request);
                case ApiKeyRequestType.ApiVersions:
                    return EncodeRequest(context, (ApiVersionsRequest) request);

                default:
                    using (var message = EncodeHeader(context, request)) {
                        return message.Payload();
                    }
            }
        }

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
        private static byte[] EncodeRequest(IRequestContext context, ProduceRequest request)
        {
            int totalCompressedBytes = 0;
            var groupedPayloads = (from p in request.Payload
                                   group p by new
                                   {
                                       p.TopicName,
                                       p.PartitionId,
                                       p.Codec
                                   } into tpc
                                   select tpc).ToList();

            using (var message = EncodeHeader(context, request)) {
                message.Pack(request.Acks)
                        .Pack((int)request.Timeout.TotalMilliseconds)
                        .Pack(groupedPayloads.Count);

                foreach (var groupedPayload in groupedPayloads) {
                    var payloads = groupedPayload.ToList();
                    message.Pack(groupedPayload.Key.TopicName, StringPrefixEncoding.Int16)
                            .Pack(payloads.Count)
                            .Pack(groupedPayload.Key.PartitionId);

                    switch (groupedPayload.Key.Codec)
                    {
                        case MessageCodec.CodecNone:
                            message.Pack(EncodeMessageSet(payloads.SelectMany(x => x.Messages)));
                            break;

                        case MessageCodec.CodecGzip:
                            var compressedBytes = CreateGzipCompressedMessage(payloads.SelectMany(x => x.Messages));
                            Interlocked.Add(ref totalCompressedBytes, compressedBytes.CompressedAmount);
                            message.Pack(EncodeMessageSet(new[] { compressedBytes.CompressedMessage }));
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
            var messageSet = EncodeMessageSet(messages);
            var gZipBytes = Compression.Zip(messageSet);

            var compressedMessage = new Message(gZipBytes, (byte)(0x00 | (MessageAttributeCodeMask & (byte)MessageCodec.CodecGzip)));

            return new CompressedMessageResult {
                CompressedAmount = messageSet.Length - compressedMessage.Value.Length,
                CompressedMessage = compressedMessage
            };
        }

        /// <summary>
        ///  The lowest 2 bits contain the compression codec used for the message. The other bits should be set to 0.
        /// </summary>
        public static byte MessageAttributeCodeMask = 0x03;

        private const int MessageHeaderSize = 12;

        private const long InitialMessageOffset = 0;

        /// <summary>
        /// Encodes a collection of messages into one byte[].  Encoded in order of list.
        /// </summary>
        /// <param name="messages">The collection of messages to encode together.</param>
        /// <returns>Encoded byte[] representing the collection of messages.</returns>
        public static byte[] EncodeMessageSet(IEnumerable<Message> messages)
        {
            using (var stream = new KafkaMessagePacker()) {
                foreach (var message in messages) {
                    stream.Pack(InitialMessageOffset)
                        .Pack(EncodeMessage(message));
                }

                return stream.PayloadNoLength();
            }
        }

        /// <summary>
        /// Encodes a message object to byte[]
        /// </summary>
        /// <param name="message">Message data to encode.</param>
        /// <returns>Encoded byte[] representation of the message object.</returns>
        /// <remarks>
        /// Format:
        /// Crc (Int32), MagicByte (Byte), Attribute (Byte), Key (Byte[]), Value (Byte[])
        /// </remarks>
        public static byte[] EncodeMessage(Message message)
        {
            using (var stream = new KafkaMessagePacker()) {
                stream.Pack(message.MessageVersion)
                      .Pack(message.Attribute);
                if (message.MessageVersion >= 1) {
                    stream.Pack(message.Timestamp.GetValueOrDefault(DateTime.UtcNow).ToUnixEpochMilliseconds());
                }
                return stream.Pack(message.Key)
                      .Pack(message.Value)
                      .CrcPayload();
            }
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
        private static byte[] EncodeRequest(IRequestContext context, FetchRequest request)
        {
            using (var message = EncodeHeader(context, request)) {
                var topicGroups = request.Fetches.GroupBy(x => x.TopicName).ToList();
                message.Pack(ReplicaId)
                        .Pack((int)Math.Min(int.MaxValue, request.MaxWaitTime.TotalMilliseconds))
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
        private static byte[] EncodeRequest(IRequestContext context, OffsetRequest request)
        {
            using (var message = EncodeHeader(context, request)) {
                var topicGroups = request.Offsets.GroupBy(x => x.TopicName).ToList();
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
        private static byte[] EncodeRequest(IRequestContext context, MetadataRequest request)
        {
            using (var message = EncodeHeader(context, request)) {
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
        private static byte[] EncodeRequest(IRequestContext context, OffsetCommitRequest request)
        {
            using (var message = EncodeHeader(context, request)) {
                message.Pack(request.ConsumerGroup, StringPrefixEncoding.Int16);
                if (context.ApiVersion >= 1) {
                    message.Pack(request.GenerationId)
                            .Pack(request.MemberId, StringPrefixEncoding.Int16);
                }
                if (context.ApiVersion >= 2) {
                    if (request.OffsetRetention.HasValue) {
                        message.Pack((long) request.OffsetRetention.Value.TotalMilliseconds);
                    } else {
                        message.Pack(-1L);
                    }
                }

                var topicGroups = request.OffsetCommits.GroupBy(x => x.TopicName).ToList();
                message.Pack(topicGroups.Count);

                foreach (var topicGroup in topicGroups) {
                    var partitions = topicGroup.GroupBy(x => x.PartitionId).ToList();
                    message.Pack(topicGroup.Key, StringPrefixEncoding.Int16)
                            .Pack(partitions.Count);

                    foreach (var partition in partitions) {
                        foreach (var commit in partition) {
                            message.Pack(partition.Key)
                                    .Pack(commit.Offset);
                            if (context.ApiVersion == 1) {
                                message.Pack(commit.TimeStamp.GetValueOrDefault(-1));
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
        private static byte[] EncodeRequest(IRequestContext context, OffsetFetchRequest request)
        {
            using (var message = EncodeHeader(context, request)) {
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
        private static byte[] EncodeRequest(IRequestContext context, GroupCoordinatorRequest request)
        {
            using (var message = EncodeHeader(context, request)) {
                message.Pack(request.ConsumerGroup, StringPrefixEncoding.Int16);
                return message.Payload();
            }
        }

        /// <summary>
        /// ApiVersions => 
        ///
        /// From http://kafka.apache.org/protocol.html#protocol_messages
        /// </summary>
        private static byte[] EncodeRequest(IRequestContext context, ApiVersionsRequest request)
        {
            using (var message = EncodeHeader(context, request)) {
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
        private static KafkaMessagePacker EncodeHeader(IRequestContext context, IKafkaRequest request)
        {
            return new KafkaMessagePacker()
                .Pack((short)request.ApiKey)
                 .Pack(context.ApiVersion)
                 .Pack(context.CorrelationId)
                 .Pack(context.ClientId, StringPrefixEncoding.Int16);
        }

        #endregion

        #region Decode

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
        private static IKafkaResponse ProduceResponse(IRequestContext context, byte[] data)
        {
            using (var stream = new BigEndianBinaryReader(data, 4)) {
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

                        if (context.ApiVersion >= 2) {
                            var milliseconds = stream.ReadInt64();
                            if (milliseconds >= 0) {
                                timestamp = milliseconds.FromUnixEpochMilliseconds();
                            }
                        }

                        topics.Add(new ProduceTopic(topicName, partitionId, errorCode, offset, timestamp));
                    }
                }

                if (context.ApiVersion >= 1) {
                    throttleTime = TimeSpan.FromMilliseconds(stream.ReadInt32());
                }
                return new ProduceResponse(topics, throttleTime);
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
        private static IKafkaResponse FetchResponse(IRequestContext context, byte[] payload)
        {
            using (var stream = new BigEndianBinaryReader(payload, 4)) {
                TimeSpan? throttleTime = null;

                if (context.ApiVersion >= 1) {
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
                        var messages = DecodeMessageSet(stream.ReadIntPrefixedBytes(), partitionId).ToList();
                        topics.Add(new FetchTopicResponse(topicName, partitionId, highWaterMarkOffset, errorCode, messages));
                    }
                }
                return new FetchResponse(topics, throttleTime);
            }
        }

        /// <summary>
        /// Decode a byte[] that represents a collection of messages.
        /// </summary>
        /// <param name="messageSet">The byte[] encode as a message set from kafka.</param>
        /// <param name="partitionId">The partitionId messages are being read from.</param>
        /// <returns>Enumerable representing stream of messages decoded from byte[]</returns>
        public static IEnumerable<Message> DecodeMessageSet(byte[] messageSet, int partitionId = 0)
        {
            using (var stream = new BigEndianBinaryReader(messageSet))
            {
                while (stream.HasData)
                {
                    //this checks that we have at least the minimum amount of data to retrieve a header
                    if (stream.Available(MessageHeaderSize) == false)
                        yield break;

                    var offset = stream.ReadInt64();
                    var messageSize = stream.ReadInt32();

                    //if messagessize is greater than the total payload, our max buffer is insufficient.
                    if ((stream.Length - MessageHeaderSize) < messageSize)
                        throw new BufferUnderRunException(MessageHeaderSize, messageSize, stream.Length);

                    //if the stream does not have enough left in the payload, we got only a partial message
                    if (stream.Available(messageSize) == false)
                        yield break;

                    foreach (var message in DecodeMessage(offset, stream.RawRead(messageSize), partitionId))
                    {
                        yield return message;
                    }
                }
            }
        }

        /// <summary>
        /// Decode messages from a payload and assign it a given kafka offset.
        /// </summary>
        /// <param name="offset">The offset represting the log entry from kafka of this message.</param>
        /// <param name="partitionId">The partition being read</param>
        /// <param name="payload">The byte[] encode as a message from kafka.</param>
        /// <returns>Enumerable representing stream of messages decoded from byte[].</returns>
        /// <remarks>The return type is an Enumerable as the message could be a compressed message set.</remarks>
        public static IEnumerable<Message> DecodeMessage(long offset, byte[] payload, int partitionId = 0)
        {
            var crc = BitConverter.ToUInt32(payload.Take(4).ToArray(), 0);
            using (var stream = new BigEndianBinaryReader(payload, 4))
            {
                var crcHash = BitConverter.ToUInt32(stream.CrcHash(), 0);
                if (crc != crcHash)
                    throw new CrcValidationException("Buffer did not match CRC validation.") { Crc = crc, CalculatedCrc = crcHash };

                var messageVersion = stream.ReadByte();
                var attribute = stream.ReadByte();
                DateTime? timestamp = null;
                if (messageVersion >= 1) {
                    var milliseconds = stream.ReadInt64();
                    if (milliseconds >= 0) {
                        timestamp = milliseconds.FromUnixEpochMilliseconds();
                    }
                }
                var key = stream.ReadIntPrefixedBytes();

                var codec = (MessageCodec)(MessageAttributeCodeMask & attribute);
                switch (codec)
                {
                    case MessageCodec.CodecNone:
                        yield return new Message(stream.ReadIntPrefixedBytes(), attribute, offset, partitionId, messageVersion, key, timestamp);
                        break;

                    case MessageCodec.CodecGzip:
                        var gZipData = stream.ReadIntPrefixedBytes();
                        foreach (var m in DecodeMessageSet(Compression.Unzip(gZipData), partitionId)) {
                            yield return m;
                        }
                        break;

                    default:
                        throw new NotSupportedException($"Codec type of {codec} is not supported.");
                }
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
        private static IKafkaResponse OffsetResponse(IRequestContext context, byte[] payload)
        {
            using (var stream = new BigEndianBinaryReader(payload, 4)) {
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
                return new OffsetResponse(topics);
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
        private static IKafkaResponse MetadataResponse(IRequestContext context, byte[] payload)
        {
            using (var stream = new BigEndianBinaryReader(payload, 4)) {
                var brokers = new List<Broker>();
                var brokerCount = stream.ReadInt32();
                for (var b = 0; b < brokerCount; b++) {
                    var brokerId = stream.ReadInt32();
                    var host = stream.ReadInt16String();
                    var port = stream.ReadInt32();

                    brokers.Add(new Broker(brokerId, host, port));
                }

                var topics = new List<MetadataTopic>();
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
                        var replicas = replicaCount.Repeat(stream.ReadInt32).ToArray();

                        var isrCount = stream.ReadInt32();
                        var isrs = isrCount.Repeat(stream.ReadInt32).ToArray();

                        partitions.Add(new MetadataPartition(partitionId, leaderId, partitionError, replicas, isrs));

                    }
                    topics.Add(new MetadataTopic(topicName, topicError, partitions));
                }

                return new MetadataResponse(brokers, topics);
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
        private static IKafkaResponse OffsetCommitResponse(IRequestContext context, byte[] payload)
        {
            using (var stream = new BigEndianBinaryReader(payload, 4)) {
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

                return new OffsetCommitResponse(topics);
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
        private static IKafkaResponse OffsetFetchResponse(IRequestContext context, byte[] payload)
        {
            using (var stream = new BigEndianBinaryReader(payload, 4)) {
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

                return new OffsetFetchResponse(topics);
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
        private static IKafkaResponse GroupCoordinatorResponse(IRequestContext context, byte[] payload)
        {
            using (var stream = new BigEndianBinaryReader(payload, 4)) {
                var errorCode = (ErrorResponseCode)stream.ReadInt16();
                var coordinatorId = stream.ReadInt32();
                var coordinatorHost = stream.ReadInt16String();
                var coordinatorPort = stream.ReadInt32();

                return new GroupCoordinatorResponse(errorCode, coordinatorId, coordinatorHost, coordinatorPort);
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
        private static IKafkaResponse ApiVersionsResponse(IRequestContext context, byte[] payload)
        {
            using (var stream = new BigEndianBinaryReader(payload, 4)) {
                var errorCode = (ErrorResponseCode)stream.ReadInt16();

                var apiKeys = new ApiVersionSupport[stream.ReadInt32()];
                for (var i = 0; i < apiKeys.Length; i++) {
                    var apiKey = (ApiKeyRequestType)stream.ReadInt16();
                    var minVersion = stream.ReadInt16();
                    var maxVersion = stream.ReadInt16();
                    apiKeys[i] = new ApiVersionSupport(apiKey, minVersion, maxVersion);
                }
                return new ApiVersionsResponse(errorCode, apiKeys);
            }
        }        

        #endregion

    }
}