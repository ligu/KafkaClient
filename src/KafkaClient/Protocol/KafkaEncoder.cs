using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using KafkaClient.Common;
using KafkaClient.Connections;
using KafkaClient.Protocol.Types;

namespace KafkaClient.Protocol
{
    [SuppressMessage("ReSharper", "UnusedParameter.Local")]
    public static class KafkaEncoder
    {
        public static T Decode<T>(IRequestContext context, byte[] payload, bool hasSize = false) where T : class, IResponse
        {
            if (typeof(T) == typeof(ProduceResponse)) return (T)ProduceResponse(context, payload, hasSize);
            if (typeof(T) == typeof(FetchResponse)) return (T)FetchResponse(context, payload, hasSize);
            if (typeof(T) == typeof(OffsetResponse)) return (T)OffsetResponse(context, payload, hasSize);
            if (typeof(T) == typeof(MetadataResponse)) return (T)MetadataResponse(context, payload, hasSize);
            if (typeof(T) == typeof(StopReplicaResponse)) return (T)StopReplicaResponse(context, payload, hasSize);
            if (typeof(T) == typeof(OffsetCommitResponse)) return (T)OffsetCommitResponse(context, payload, hasSize);
            if (typeof(T) == typeof(OffsetFetchResponse)) return (T)OffsetFetchResponse(context, payload, hasSize);
            if (typeof(T) == typeof(GroupCoordinatorResponse)) return (T)GroupCoordinatorResponse(context, payload, hasSize);
            if (typeof(T) == typeof(JoinGroupResponse)) return (T)JoinGroupResponse(context, payload, hasSize);
            if (typeof(T) == typeof(HeartbeatResponse)) return (T)HeartbeatResponse(context, payload, hasSize);
            if (typeof(T) == typeof(LeaveGroupResponse)) return (T)LeaveGroupResponse(context, payload, hasSize);
            if (typeof(T) == typeof(SyncGroupResponse)) return (T)SyncGroupResponse(context, payload, hasSize);
            if (typeof(T) == typeof(DescribeGroupsResponse)) return (T)DescribeGroupsResponse(context, payload, hasSize);
            if (typeof(T) == typeof(ListGroupsResponse)) return (T)ListGroupsResponse(context, payload, hasSize);
            if (typeof(T) == typeof(SaslHandshakeResponse)) return (T)SaslHandshakeResponse(context, payload, hasSize);
            if (typeof(T) == typeof(ApiVersionsResponse)) return (T)ApiVersionsResponse(context, payload, hasSize);
            return default(T);
        }

        public static DataPayload Encode<T>(IRequestContext context, T request) where T : class, IRequest
        {
            switch (request.ApiKey) {
                case ApiKeyRequestType.Produce: {
                    var produceRequest = (ProduceRequest)(IRequest)request;
                    return new DataPayload(
                        EncodeRequest(context, produceRequest), 
                        context.CorrelationId, 
                        request.ApiKey, 
                        produceRequest.Payloads.Sum(x => x.Messages.Count));
                }

                default:
                    return new DataPayload(EncodeRequestBytes(context, request), context.CorrelationId, request.ApiKey);
            }
        }

        #region Encode

        internal static byte[] EncodeRequestBytes(IRequestContext context, IRequest request)
        {
            switch (request.ApiKey) {
                case ApiKeyRequestType.Produce:
                    return EncodeRequest(context, (ProduceRequest) request);
                case ApiKeyRequestType.Fetch:
                    return EncodeRequest(context, (FetchRequest) request);
                case ApiKeyRequestType.Offset:
                    return EncodeRequest(context, (OffsetRequest) request);
                case ApiKeyRequestType.Metadata:
                    return EncodeRequest(context, (MetadataRequest) request);
                case ApiKeyRequestType.StopReplica:
                    return EncodeRequest(context, (StopReplicaRequest) request);
                case ApiKeyRequestType.OffsetCommit:
                    return EncodeRequest(context, (OffsetCommitRequest) request);
                case ApiKeyRequestType.OffsetFetch:
                    return EncodeRequest(context, (OffsetFetchRequest) request);
                case ApiKeyRequestType.GroupCoordinator:
                    return EncodeRequest(context, (GroupCoordinatorRequest) request);
                case ApiKeyRequestType.JoinGroup:
                    return EncodeRequest(context, (JoinGroupRequest) request);
                case ApiKeyRequestType.Heartbeat:
                    return EncodeRequest(context, (HeartbeatRequest) request);
                case ApiKeyRequestType.LeaveGroup:
                    return EncodeRequest(context, (LeaveGroupRequest) request);
                case ApiKeyRequestType.SyncGroup:
                    return EncodeRequest(context, (SyncGroupRequest) request);
                case ApiKeyRequestType.DescribeGroups:
                    return EncodeRequest(context, (DescribeGroupsRequest) request);
                case ApiKeyRequestType.ListGroups:
                    return EncodeRequest(context, (ListGroupsRequest) request);
                case ApiKeyRequestType.SaslHandshake:
                    return EncodeRequest(context, (SaslHandshakeRequest) request);
                case ApiKeyRequestType.ApiVersions:
                    return EncodeRequest(context, (ApiVersionsRequest) request);

                default:
                    using (var message = EncodeHeader(context, request)) {
                        return message.ToBytes();
                    }
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
            using (var stream = new MessagePacker()) {
                foreach (var message in messages) {
                    stream.Pack(InitialMessageOffset)
                        .Pack(EncodeMessage(message));
                }

                return stream.ToBytesNoLength();
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
            using (var stream = new MessagePacker()) {
                stream.Pack(message.MessageVersion)
                      .Pack(message.Attribute);
                if (message.MessageVersion >= 1) {
                    stream.Pack(message.Timestamp.GetValueOrDefault(DateTime.UtcNow).ToUnixEpochMilliseconds());
                }
                return stream.Pack(message.Key)
                      .Pack(message.Value)
                      .ToBytesCrc();
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

        private static byte[] EncodeRequest(IRequestContext context, ProduceRequest request)
        {
            var totalCompressedBytes = 0;
            var groupedPayloads = (from p in request.Payloads
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
                    message.Pack(groupedPayload.Key.TopicName)
                            .Pack(payloads.Count) // shouldn't this be 1?
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

                var bytes = message.ToBytes();
                StatisticsTracker.RecordProduceRequest(request.Payloads.Sum(x => x.Messages.Count), bytes.Length, totalCompressedBytes);
                return bytes;
            }
        }

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
                    message.Pack(topicGroup.Key)
                            .Pack(partitions.Count);

                    foreach (var partition in partitions) {
                        foreach (var fetch in partition) {
                            message.Pack(partition.Key)
                                    .Pack(fetch.Offset)
                                    .Pack(fetch.MaxBytes);
                        }
                    }
                }

                return message.ToBytes();
            }
        }

        private static byte[] EncodeRequest(IRequestContext context, OffsetRequest request)
        {
            using (var message = EncodeHeader(context, request)) {
                var topicGroups = request.Offsets.GroupBy(x => x.TopicName).ToList();
                message.Pack(ReplicaId)
                        .Pack(topicGroups.Count);

                foreach (var topicGroup in topicGroups) {
                    var partitions = topicGroup.GroupBy(x => x.PartitionId).ToList();
                    message.Pack(topicGroup.Key)
                            .Pack(partitions.Count);

                    foreach (var partition in partitions) {
                        foreach (var offset in partition) {
                            message.Pack(partition.Key)
                                    .Pack(offset.Time)
                                    .Pack(offset.MaxOffsets);
                        }
                    }
                }

                return message.ToBytes();
            }
        }

        private static byte[] EncodeRequest(IRequestContext context, MetadataRequest request)
        {
            using (var message = EncodeHeader(context, request)) {
                message.Pack(request.Topics.Count)
                        .Pack(request.Topics);

                return message.ToBytes();
            }
        }

        private static byte[] EncodeRequest(IRequestContext context, StopReplicaRequest request)
        {
            using (var message = EncodeHeader(context, request)) {
                message.Pack(request.ControllerId)
                    .Pack(request.ControllerEpoch)
                    .Pack(request.DeletePartitions ? (byte)1 : (byte)0)
                    .Pack(request.Topics.Count);

                foreach (var topic in request.Topics) {
                    message.Pack(topic.TopicName)
                           .Pack(topic.PartitionId);
                }

                return message.ToBytes();
            }
        }

        private static byte[] EncodeRequest(IRequestContext context, OffsetCommitRequest request)
        {
            using (var message = EncodeHeader(context, request)) {
                message.Pack(request.GroupId);
                if (context.ApiVersion >= 1) {
                    message.Pack(request.GenerationId)
                            .Pack(request.MemberId);
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
                    message.Pack(topicGroup.Key)
                            .Pack(partitions.Count);

                    foreach (var partition in partitions) {
                        foreach (var commit in partition) {
                            message.Pack(partition.Key)
                                    .Pack(commit.Offset);
                            if (context.ApiVersion == 1) {
                                message.Pack(commit.TimeStamp.GetValueOrDefault(-1));
                            }
                            message.Pack(commit.Metadata);
                        }
                    }
                }
                return message.ToBytes();
            }
        }

        private static byte[] EncodeRequest(IRequestContext context, OffsetFetchRequest request)
        {
            using (var message = EncodeHeader(context, request)) {
                var topicGroups = request.Topics.GroupBy(x => x.TopicName).ToList();

                message.Pack(request.GroupId)
                        .Pack(topicGroups.Count);

                foreach (var topicGroup in topicGroups) {
                    var partitions = topicGroup.GroupBy(x => x.PartitionId).ToList();
                    message.Pack(topicGroup.Key)
                            .Pack(partitions.Count);

                    foreach (var partition in partitions) {
                        foreach (var offset in partition) {
                            message.Pack(offset.PartitionId);
                        }
                    }
                }

                return message.ToBytes();
            }
        }

        private static byte[] EncodeRequest(IRequestContext context, GroupCoordinatorRequest request)
        {
            using (var message = EncodeHeader(context, request)) {
                message.Pack(request.GroupId);
                return message.ToBytes();
            }
        }

        private static byte[] EncodeRequest(IRequestContext context, JoinGroupRequest request)
        {
            using (var message = EncodeHeader(context, request)) {
                message.Pack(request.GroupId)
                    .Pack((int)request.SessionTimeout.TotalMilliseconds)
                    .Pack(request.MemberId)
                    .Pack(request.ProtocolType)
                    .Pack(request.GroupProtocols.Count);

                foreach (var protocol in request.GroupProtocols) {
                    message.Pack(protocol.Name)
                           .Pack(protocol.Metadata);
                }

                return message.ToBytes();
            }
        }

        private static byte[] EncodeRequest(IRequestContext context, HeartbeatRequest request)
        {
            using (var message = EncodeHeader(context, request)) {
                message.Pack(request.GroupId)
                    .Pack(request.GenerationId)
                    .Pack(request.MemberId);

                return message.ToBytes();
            }
        }

        private static byte[] EncodeRequest(IRequestContext context, LeaveGroupRequest request)
        {
            using (var message = EncodeHeader(context, request)) {
                message.Pack(request.GroupId)
                    .Pack(request.MemberId);

                return message.ToBytes();
            }
        }

        private static byte[] EncodeRequest(IRequestContext context, SyncGroupRequest request)
        {
            using (var message = EncodeHeader(context, request)) {
                message.Pack(request.GroupId)
                    .Pack(request.GenerationId)
                    .Pack(request.MemberId)
                    .Pack(request.GroupAssignments.Count);

                foreach (var assignment in request.GroupAssignments) {
                    message.Pack(assignment.MemberId)
                           .Pack(assignment.MemberAssignment);
                }

                return message.ToBytes();
            }
        }

        private static byte[] EncodeRequest(IRequestContext context, DescribeGroupsRequest request)
        {
            using (var message = EncodeHeader(context, request)) {
                message.Pack(request.GroupIds.Count);

                foreach (var groupId in request.GroupIds) {
                    message.Pack(groupId);
                }

                return message.ToBytes();
            }
        }

        private static byte[] EncodeRequest(IRequestContext context, ListGroupsRequest request)
        {
            using (var message = EncodeHeader(context, request)) {
                return message.ToBytes();
            }
        }

        private static byte[] EncodeRequest(IRequestContext context, SaslHandshakeRequest request)
        {
            using (var message = EncodeHeader(context, request)) {
                message.Pack(request.Mechanism);

                return message.ToBytes();
            }
        }

        private static byte[] EncodeRequest(IRequestContext context, ApiVersionsRequest request)
        {
            using (var message = EncodeHeader(context, request)) {
                return message.ToBytes();
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
        private static MessagePacker EncodeHeader(IRequestContext context, IRequest request)
        {
            return new MessagePacker()
                .Pack((short)request.ApiKey)
                 .Pack(context.ApiVersion.GetValueOrDefault())
                 .Pack(context.CorrelationId)
                 .Pack(context.ClientId);
        }

        #endregion

        #region Decode

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
                    if (stream.Length - MessageHeaderSize < messageSize)
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
                if (crc != crcHash) throw new CrcValidationException("Buffer did not match CRC validation.") { Crc = crc, CalculatedCrc = crcHash };

                var messageVersion = stream.ReadByte();
                var attribute = stream.ReadByte();
                DateTime? timestamp = null;
                if (messageVersion >= 1) {
                    var milliseconds = stream.ReadInt64();
                    if (milliseconds >= 0) {
                        timestamp = milliseconds.FromUnixEpochMilliseconds();
                    }
                }
                var key = stream.ReadBytes();

                var codec = (MessageCodec)(MessageAttributeCodeMask & attribute);
                switch (codec)
                {
                    case MessageCodec.CodecNone:
                        yield return new Message(stream.ReadBytes(), attribute, offset, partitionId, messageVersion, key, timestamp);
                        break;

                    case MessageCodec.CodecGzip:
                        var gZipData = stream.ReadBytes();
                        foreach (var m in DecodeMessageSet(Compression.Unzip(gZipData), partitionId)) {
                            yield return m;
                        }
                        break;

                    default:
                        throw new NotSupportedException($"Codec type of {codec} is not supported.");
                }
            }
        }

        private static IResponse ProduceResponse(IRequestContext context, byte[] payload, bool hasSize)
        {
            using (var stream = new BigEndianBinaryReader(payload, hasSize ? 8 : 4)) {
                TimeSpan? throttleTime = null;

                var topics = new List<ProduceTopic>();
                var topicCount = stream.ReadInt32();
                for (var i = 0; i < topicCount; i++) {
                    var topicName = stream.ReadString();

                    var partitionCount = stream.ReadInt32();
                    for (var j = 0; j < partitionCount; j++) {
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

        private static IResponse FetchResponse(IRequestContext context, byte[] payload, bool hasSize)
        {
            using (var stream = new BigEndianBinaryReader(payload, hasSize ? 8 : 4)) {
                TimeSpan? throttleTime = null;

                if (context.ApiVersion >= 1) {
                    throttleTime = TimeSpan.FromMilliseconds(stream.ReadInt32());
                }

                var topics = new List<FetchTopicResponse>();
                var topicCount = stream.ReadInt32();
                for (var t = 0; t < topicCount; t++) {
                    var topicName = stream.ReadString();

                    var partitionCount = stream.ReadInt32();
                    for (var p = 0; p < partitionCount; p++) {
                        var partitionId = stream.ReadInt32();
                        var errorCode = (ErrorResponseCode) stream.ReadInt16();
                        var highWaterMarkOffset = stream.ReadInt64();
                        var messages = DecodeMessageSet(stream.ReadBytes(), partitionId).ToList();
                        topics.Add(new FetchTopicResponse(topicName, partitionId, highWaterMarkOffset, errorCode, messages));
                    }
                }
                return new FetchResponse(topics, throttleTime);
            }
        }

        private static IResponse OffsetResponse(IRequestContext context, byte[] payload, bool hasSize)
        {
            using (var stream = new BigEndianBinaryReader(payload, hasSize ? 8 : 4)) {
                var topics = new List<OffsetTopic>();
                var topicCount = stream.ReadInt32();
                for (var t = 0; t < topicCount; t++) {
                    var topicName = stream.ReadString();

                    var partitionCount = stream.ReadInt32();
                    for (var p = 0; p < partitionCount; p++) {
                        var partitionId = stream.ReadInt32();
                        var errorCode = (ErrorResponseCode) stream.ReadInt16();

                        var offsets = new long[stream.ReadInt32()];
                        for (var o = 0; o < offsets.Length; o++) {
                            offsets[o] = stream.ReadInt64();
                        }

                        topics.Add(new OffsetTopic(topicName, partitionId, errorCode, offsets));
                    }
                }
                return new OffsetResponse(topics);
            }
        }

        private static IResponse MetadataResponse(IRequestContext context, byte[] payload, bool hasSize)
        {
            using (var stream = new BigEndianBinaryReader(payload, hasSize ? 8 : 4)) {
                var brokers = new Broker[stream.ReadInt32()];
                for (var b = 0; b < brokers.Length; b++) {
                    var brokerId = stream.ReadInt32();
                    var host = stream.ReadString();
                    var port = stream.ReadInt32();

                    brokers[b] = new Broker(brokerId, host, port);
                }

                var topics = new MetadataTopic[stream.ReadInt32()];
                for (var t = 0; t < topics.Length; t++) {
                    var topicError = (ErrorResponseCode) stream.ReadInt16();
                    var topicName = stream.ReadString();

                    var partitions = new MetadataPartition[stream.ReadInt32()];
                    for (var p = 0; p < partitions.Length; p++) {
                        var partitionError = (ErrorResponseCode) stream.ReadInt16();
                        var partitionId = stream.ReadInt32();
                        var leaderId = stream.ReadInt32();

                        var replicaCount = stream.ReadInt32();
                        var replicas = replicaCount.Repeat(stream.ReadInt32).ToArray();

                        var isrCount = stream.ReadInt32();
                        var isrs = isrCount.Repeat(stream.ReadInt32).ToArray();

                        partitions[p] = new MetadataPartition(partitionId, leaderId, partitionError, replicas, isrs);

                    }
                    topics[t] = new MetadataTopic(topicName, topicError, partitions);
                }

                return new MetadataResponse(brokers, topics);
            }
        }

        private static IResponse StopReplicaResponse(IRequestContext context, byte[] payload, bool hasSize)
        {
            using (var stream = new BigEndianBinaryReader(payload, hasSize ? 8 : 4)) {
                var errorCode = (ErrorResponseCode)stream.ReadInt16();

                var topics = new TopicResponse[stream.ReadInt32()];
                for (var i = 0; i < topics.Length; i++) {
                    var topicName = stream.ReadString();
                    var partitionId = stream.ReadInt32();
                    var topicErrorCode = (ErrorResponseCode)stream.ReadInt16();
                    topics[i] = new TopicResponse(topicName, partitionId, topicErrorCode);
                }
                return new StopReplicaResponse(errorCode, topics);
            }
        }
        
        private static IResponse OffsetCommitResponse(IRequestContext context, byte[] payload, bool hasSize)
        {
            using (var stream = new BigEndianBinaryReader(payload, hasSize ? 8 : 4)) {
                var topics = new List<TopicResponse>();
                var topicCount = stream.ReadInt32();
                for (var t = 0; t < topicCount; t++) {
                    var topicName = stream.ReadString();

                    var partitionCount = stream.ReadInt32();
                    for (var p = 0; p < partitionCount; p++) {
                        var partitionId = stream.ReadInt32();
                        var errorCode = (ErrorResponseCode) stream.ReadInt16();

                        topics.Add(new TopicResponse(topicName, partitionId, errorCode));
                    }
                }

                return new OffsetCommitResponse(topics);
            }
        }

        private static IResponse OffsetFetchResponse(IRequestContext context, byte[] payload, bool hasSize)
        {
            using (var stream = new BigEndianBinaryReader(payload, hasSize ? 8 : 4)) {
                var topics = new List<OffsetFetchTopic>();
                var topicCount = stream.ReadInt32();
                for (var t = 0; t < topicCount; t++) {
                    var topicName = stream.ReadString();

                    var partitionCount = stream.ReadInt32();
                    for (var p = 0; p < partitionCount; p++) {
                        var partitionId = stream.ReadInt32();
                        var offset = stream.ReadInt64();
                        var metadata = stream.ReadString();
                        var errorCode = (ErrorResponseCode) stream.ReadInt16();

                        topics.Add(new OffsetFetchTopic(topicName, partitionId, errorCode, offset, metadata));
                    }
                }

                return new OffsetFetchResponse(topics);
            }
        }
        
        private static IResponse GroupCoordinatorResponse(IRequestContext context, byte[] payload, bool hasSize)
        {
            using (var stream = new BigEndianBinaryReader(payload, hasSize ? 8 : 4)) {
                var errorCode = (ErrorResponseCode)stream.ReadInt16();
                var coordinatorId = stream.ReadInt32();
                var coordinatorHost = stream.ReadString();
                var coordinatorPort = stream.ReadInt32();

                return new GroupCoordinatorResponse(errorCode, coordinatorId, coordinatorHost, coordinatorPort);
            }
        }

        private static IResponse JoinGroupResponse(IRequestContext context, byte[] payload, bool hasSize)
        {
            using (var stream = new BigEndianBinaryReader(payload, hasSize ? 8 : 4)) {
                var errorCode = (ErrorResponseCode)stream.ReadInt16();
                var generationId = stream.ReadInt32();
                var groupProtocol = stream.ReadString();
                var leaderId = stream.ReadString();
                var memberId = stream.ReadString();

                var members = new GroupMember[stream.ReadInt32()];
                for (var m = 0; m < members.Length; m++) {
                    var id = stream.ReadString();
                    var metadata = stream.ReadBytes();
                    members[m] = new GroupMember(id, metadata);
                }

                return new JoinGroupResponse(errorCode, generationId, groupProtocol, leaderId, memberId, members);
            }
        }

        private static IResponse HeartbeatResponse(IRequestContext context, byte[] payload, bool hasSize)
        {
            using (var stream = new BigEndianBinaryReader(payload, hasSize ? 8 : 4)) {
                var errorCode = (ErrorResponseCode)stream.ReadInt16();

                return new HeartbeatResponse(errorCode);
            }
        }

        private static IResponse LeaveGroupResponse(IRequestContext context, byte[] payload, bool hasSize)
        {
            using (var stream = new BigEndianBinaryReader(payload, hasSize ? 8 : 4)) {
                var errorCode = (ErrorResponseCode)stream.ReadInt16();

                return new LeaveGroupResponse(errorCode);
            }
        }

        private static IResponse SyncGroupResponse(IRequestContext context, byte[] payload, bool hasSize)
        {
            using (var stream = new BigEndianBinaryReader(payload, hasSize ? 8 : 4)) {
                var errorCode = (ErrorResponseCode)stream.ReadInt16();
                var memberAssignment = stream.ReadBytes();

                return new SyncGroupResponse(errorCode, memberAssignment);
            }
        }

        private static IResponse DescribeGroupsResponse(IRequestContext context, byte[] payload, bool hasSize)
        {
            using (var stream = new BigEndianBinaryReader(payload, hasSize ? 8 : 4)) {
                var groups = new DescribeGroup[stream.ReadInt32()];
                for (var g = 0; g < groups.Length; g++) {
                    var errorCode = (ErrorResponseCode)stream.ReadInt16();
                    var groupId = stream.ReadString();
                    var state = stream.ReadString();
                    var protocolType = stream.ReadString();
                    var protocol = stream.ReadString();
                    var members = new DescribeGroupMember[stream.ReadInt32()];
                    for (var m = 0; m < members.Length; m++) {
                        var memberId = stream.ReadString();
                        var clientId = stream.ReadString();
                        var clientHost = stream.ReadString();
                        var memberMetadata = stream.ReadBytes();
                        var memberAssignment = stream.ReadBytes();
                        members[m] = new DescribeGroupMember(memberId, clientId, clientHost, memberMetadata, memberAssignment);
                    }
                    groups[g] = new DescribeGroup(errorCode, groupId, state, protocolType, protocol, members);
                }

                return new DescribeGroupsResponse(groups);
            }
        }

        private static IResponse ListGroupsResponse(IRequestContext context, byte[] payload, bool hasSize)
        {
            using (var stream = new BigEndianBinaryReader(payload, hasSize ? 8 : 4)) {
                var errorCode = (ErrorResponseCode)stream.ReadInt16();
                var groups = new ListGroup[stream.ReadInt32()];
                for (var g = 0; g < groups.Length; g++) {
                    var groupId = stream.ReadString();
                    var protocolType = stream.ReadString();
                    groups[g] = new ListGroup(groupId, protocolType);
                }

                return new ListGroupsResponse(errorCode, groups);
            }
        }

        private static IResponse SaslHandshakeResponse(IRequestContext context, byte[] payload, bool hasSize)
        {
            using (var stream = new BigEndianBinaryReader(payload, hasSize ? 8 : 4)) {
                var errorCode = (ErrorResponseCode)stream.ReadInt16();
                var enabledMechanisms = new string[stream.ReadInt32()];
                for (var m = 0; m < enabledMechanisms.Length; m++) {
                    enabledMechanisms[m] = stream.ReadString();
                }

                return new SaslHandshakeResponse(errorCode, enabledMechanisms);
            }
        }

        private static IResponse ApiVersionsResponse(IRequestContext context, byte[] payload, bool hasSize)
        {
            using (var stream = new BigEndianBinaryReader(payload, hasSize ? 8 : 4)) {
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