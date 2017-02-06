using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Assignment;
using KafkaClient.Common;
using KafkaClient.Connections;

namespace KafkaClient.Protocol
{
    public static class Extensions
    {
        #region Exception

        public static Exception ExtractExceptions<TResponse>(this IRequest<TResponse> request, TResponse response, Endpoint endpoint = null) where TResponse : IResponse
        {
            var exceptions = new List<Exception>();
            if (response != null) {
                foreach (var errorCode in response.Errors.Where(e => !e.IsSuccess())) {
                    exceptions.Add(ExtractException(request, errorCode, endpoint));
                }
            }
            if (exceptions.Count == 0) return new RequestException(request.ApiKey, ErrorCode.NONE, endpoint);
            if (exceptions.Count == 1) return exceptions[0];
            return new AggregateException(exceptions);
        }

        public static Exception ExtractException(this IRequest request, ErrorCode errorCode, Endpoint endpoint) 
        {
            return ExtractFetchException(request as FetchRequest, errorCode, endpoint) ??
                   ExtractMemberException(request, errorCode, endpoint) ??
                   new RequestException(request.ApiKey, errorCode, endpoint);
        }

        private static MemberRequestException ExtractMemberException(IRequest request, ErrorCode errorCode, Endpoint endpoint)
        {
            var member = request as IGroupMember;
            if (member != null && 
                (errorCode == ErrorCode.UNKNOWN_MEMBER_ID ||
                errorCode == ErrorCode.ILLEGAL_GENERATION || 
                errorCode == ErrorCode.INCONSISTENT_GROUP_PROTOCOL))
            {
                return new MemberRequestException(member, request.ApiKey, errorCode, endpoint);
            }
            return null;
        } 

        private static FetchOutOfRangeException ExtractFetchException(FetchRequest request, ErrorCode errorCode, Endpoint endpoint)
        {
            if (errorCode == ErrorCode.OFFSET_OUT_OF_RANGE && request?.topics?.Count == 1) {
                return new FetchOutOfRangeException(request.topics.First(), errorCode, endpoint);
            }
            return null;
        }        

        #endregion

        #region Encoding

        public static IMembershipEncoder GetEncoder(this IRequestContext context, string protocolType = null)
        {
            var type = protocolType ?? context.ProtocolType;
            IMembershipEncoder encoder;
            if (type != null && context.Encoders != null && context.Encoders.TryGetValue(type, out encoder) && encoder != null) return encoder;

            throw new ArgumentOutOfRangeException(nameof(protocolType), $"Unknown protocol type {protocolType}");
        }

        public static IKafkaWriter Write(this IKafkaWriter writer, IMemberMetadata metadata, IMembershipEncoder encoder)
        {
            encoder.EncodeMetadata(writer, metadata);
            return writer;
        }

        public static IKafkaWriter Write(this IKafkaWriter writer, IMemberAssignment assignment, IMembershipEncoder encoder)
        {
            encoder.EncodeAssignment(writer, assignment);
            return writer;
        }

        public static IKafkaWriter Write(this IKafkaWriter writer, IImmutableList<int> values)
        {
            writer.Write(values.Count);
            foreach (var value in values) {
                writer.Write(value);
            }
            return writer;
        }

        public static IKafkaWriter Write(this IKafkaWriter writer, ErrorCode errorCode)
        {
            return writer.Write((short)errorCode);
        }

        public static IKafkaWriter Write(this IKafkaWriter writer, IEnumerable<string> values, bool includeLength = false)
        {
            if (includeLength) {
                var valuesList = values.ToList();
                writer.Write(valuesList.Count);
                writer.Write(valuesList); // NOTE: !includeLength passed next time
                return writer;
            }

            foreach (var item in values) {
                writer.Write(item);
            }
            return writer;
        }

        public static IKafkaWriter Write(this IKafkaWriter writer, IEnumerable<Message> messages)
        {
            foreach (var message in messages) {
                writer.Write(0L);
                using (writer.MarkForLength()) {
                    message.WriteTo(writer);
                }
            }
            return writer;
        }

        public static int Write(this IKafkaWriter writer, IEnumerable<Message> messages, MessageCodec codec)
        {
            if (codec == MessageCodec.None) {
                using (writer.MarkForLength()) {
                    writer.Write(messages);
                }
                return 0;
            }
            using (var messageWriter = new KafkaWriter()) {
                messageWriter.Write(messages);
                var messageSet = messageWriter.ToSegment(false);

                using (writer.MarkForLength()) { // messageset
                    writer.Write(0L); // offset
                    using (writer.MarkForLength()) { // message
                        using (writer.MarkForCrc()) {
                            writer.Write((byte)0) // message version
                                    .Write((byte)codec) // attribute
                                    .Write(-1); // key  -- null, so -1 length
                            using (writer.MarkForLength()) { // value
                                var initialPosition = writer.Position;
                                writer.WriteCompressed(messageSet, codec);
                                var compressedMessageLength = writer.Position - initialPosition;
                                return messageSet.Count - compressedMessageLength;
                            }
                        }
                    }
                }
            }
        }

        #endregion

        #region Decoding

        private const int MessageHeaderSize = 12;

        /// <summary>
        /// Decode a byte[] that represents a collection of messages.
        /// </summary>
        /// <param name="reader">The reader</param>
        /// <param name="codec">The codec of the containing messageset, if any</param>
        /// <returns>Enumerable representing stream of messages decoded from byte[]</returns>
        public static IImmutableList<Message> ReadMessages(this IKafkaReader reader, MessageCodec codec = MessageCodec.None)
        {
            var expectedLength = reader.ReadInt32();
            if (!reader.HasBytes(expectedLength)) throw new BufferUnderRunException($"Message set size of {expectedLength} is not fully available (codec {codec}).");

            var messages = ImmutableList<Message>.Empty;
            var finalPosition = reader.Position + expectedLength;
            while (reader.Position < finalPosition) {
                // this checks that we have at least the minimum amount of data to retrieve a header
                if (reader.HasBytes(MessageHeaderSize) == false) break;

                var offset = reader.ReadInt64();
                var messageSize = reader.ReadInt32();

                // if the stream does not have enough left in the payload, we got only a partial message
                if (reader.HasBytes(messageSize) == false) throw new BufferUnderRunException($"Message header size of {MessageHeaderSize} is not fully available (codec {codec}).");

                try {
                    messages = messages.AddRange(reader.ReadMessage(messageSize, offset));
                } catch (EndOfStreamException ex) {
                    throw new BufferUnderRunException($"Message size of {messageSize} is not available (codec {codec}).", ex);
                }
            }
            return messages;
        }

        /// <summary>
        /// Decode messages from a payload and assign it a given kafka offset.
        /// </summary>
        /// <param name="reader">The reader</param>
        /// <param name="messageSize">The size of the message, for Crc Hash calculation</param>
        /// <param name="offset">The offset represting the log entry from kafka of this message.</param>
        /// <returns>Enumerable representing stream of messages decoded from byte[].</returns>
        /// <remarks>The return type is an Enumerable as the message could be a compressed message set.</remarks>
        public static IImmutableList<Message> ReadMessage(this IKafkaReader reader, int messageSize, long offset)
        {
            var crc = reader.ReadUInt32();
            var crcHash = reader.ReadCrc(messageSize - 4);
            if (crc != crcHash) throw new CrcValidationException(crc, crcHash);

            var messageVersion = reader.ReadByte();
            var attribute = reader.ReadByte();
            DateTimeOffset? timestamp = null;
            if (messageVersion >= 1) {
                var milliseconds = reader.ReadInt64();
                if (milliseconds >= 0) {
                    timestamp = DateTimeOffset.FromUnixTimeMilliseconds(milliseconds);
                }
            }
            var key = reader.ReadBytes();
            var value = reader.ReadBytes();

            var codec = (MessageCodec)(Message.CodecMask & attribute);
            if (codec == MessageCodec.None) {
                return ImmutableList<Message>.Empty.Add(new Message(value, key, attribute, offset, messageVersion, timestamp));
            }
            var uncompressedBytes = value.ToUncompressed(codec);
            using (var messageSetReader = new KafkaReader(uncompressedBytes)) {
                return messageSetReader.ReadMessages(codec);
            }
        }

        public static IResponse ToResponse(this ApiKey apiKey, IRequestContext context, ArraySegment<byte> bytes)
        {
            switch (apiKey) {
                case ApiKey.Produce:
                    return ProduceResponse.FromBytes(context, bytes);
                case ApiKey.Fetch:
                    return FetchResponse.FromBytes(context, bytes);
                case ApiKey.Offsets:
                    return OffsetsResponse.FromBytes(context, bytes);
                case ApiKey.Metadata:
                    return MetadataResponse.FromBytes(context, bytes);
                case ApiKey.OffsetCommit:
                    return OffsetCommitResponse.FromBytes(context, bytes);
                case ApiKey.OffsetFetch:
                    return OffsetFetchResponse.FromBytes(context, bytes);
                case ApiKey.GroupCoordinator:
                    return GroupCoordinatorResponse.FromBytes(context, bytes);
                case ApiKey.JoinGroup:
                    return JoinGroupResponse.FromBytes(context, bytes);
                case ApiKey.Heartbeat:
                    return HeartbeatResponse.FromBytes(context, bytes);
                case ApiKey.LeaveGroup:
                    return LeaveGroupResponse.FromBytes(context, bytes);
                case ApiKey.SyncGroup:
                    return SyncGroupResponse.FromBytes(context, bytes);
                case ApiKey.DescribeGroups:
                    return DescribeGroupsResponse.FromBytes(context, bytes);
                case ApiKey.ListGroups:
                    return ListGroupsResponse.FromBytes(context, bytes);
                case ApiKey.SaslHandshake:
                    return SaslHandshakeResponse.FromBytes(context, bytes);
                case ApiKey.ApiVersions:
                    return ApiVersionsResponse.FromBytes(context, bytes);
                case ApiKey.CreateTopics:
                    return CreateTopicsResponse.FromBytes(context, bytes);
                case ApiKey.DeleteTopics:
                    return DeleteTopicsResponse.FromBytes(context, bytes);
                default:
                    throw new NotImplementedException($"Unknown response type {apiKey}");
            }
        }

        public static ErrorCode ReadErrorCode(this IKafkaReader reader)
        {
            return (ErrorCode) reader.ReadInt16();
        }

        public static bool IsSuccess(this ErrorCode code)
        {
            return code == ErrorCode.NONE;
        }

        /// <summary>
        /// See http://kafka.apache.org/protocol.html#protocol_error_codes for details
        /// </summary>
        public static bool IsRetryable(this ErrorCode code)
        {
            return code == ErrorCode.CORRUPT_MESSAGE
                || code == ErrorCode.UNKNOWN_TOPIC_OR_PARTITION
                || code == ErrorCode.LEADER_NOT_AVAILABLE
                || code == ErrorCode.NOT_LEADER_FOR_PARTITION
                || code == ErrorCode.REQUEST_TIMED_OUT
                || code == ErrorCode.NETWORK_EXCEPTION
                || code == ErrorCode.GROUP_LOAD_IN_PROGRESS
                || code == ErrorCode.GROUP_COORDINATOR_NOT_AVAILABLE
                || code == ErrorCode.NOT_COORDINATOR_FOR_GROUP
                || code == ErrorCode.NOT_ENOUGH_REPLICAS
                || code == ErrorCode.NOT_ENOUGH_REPLICAS_AFTER_APPEND
                || code == ErrorCode.NOT_CONTROLLER;
        }

        public static bool IsFromStaleMetadata(this ErrorCode code)
        {
            return code == ErrorCode.UNKNOWN_TOPIC_OR_PARTITION
                || code == ErrorCode.LEADER_NOT_AVAILABLE
                || code == ErrorCode.NOT_LEADER_FOR_PARTITION
                || code == ErrorCode.GROUP_LOAD_IN_PROGRESS
                || code == ErrorCode.GROUP_COORDINATOR_NOT_AVAILABLE
                || code == ErrorCode.NOT_COORDINATOR_FOR_GROUP;
        }

        #endregion

        #region Router

        /// <summary>
        /// Get offsets for all partitions of a given topic.
        /// </summary>
        /// <param name="router">The router which provides the route and metadata.</param>
        /// <param name="topicName">Name of the topic to get offset information from.</param>
        /// <param name="maxOffsets">How many to get, at most.</param>
        /// <param name="offsetTime">These are best described by <see cref="OffsetsRequest.Topic.timestamp"/></param>
        /// <param name="cancellationToken"></param>
        public static async Task<IImmutableList<OffsetsResponse.Topic>> GetTopicOffsetsAsync(this IRouter router, string topicName, int maxOffsets, long offsetTime, CancellationToken cancellationToken)
        {
            bool? metadataInvalid = false;
            var offsets = new Dictionary<int, OffsetsResponse.Topic>();
            RoutedTopicRequest<OffsetsResponse>[] routedTopicRequests = null;

            return await router.Configuration.SendRetry.TryAsync(
                async (retryAttempt, elapsed) => {
                    metadataInvalid = await router.RefreshTopicMetadataIfInvalidAsync(topicName, metadataInvalid, cancellationToken).ConfigureAwait(false);

                    var topicMetadata = await router.GetTopicMetadataAsync(topicName, cancellationToken).ConfigureAwait(false);
                    routedTopicRequests = topicMetadata
                        .partition_metadata
                        .Where(_ => !offsets.ContainsKey(_.partition_id)) // skip partitions already successfully retrieved
                        .GroupBy(x => x.leader)
                        .Select(partitions => 
                            new RoutedTopicRequest<OffsetsResponse>(
                                new OffsetsRequest(partitions.Select(_ => new OffsetsRequest.Topic(topicName, _.partition_id, offsetTime, maxOffsets))), 
                                topicName, 
                                partitions.Select(_ => _.partition_id).First(), 
                                router.Log))
                        .ToArray();

                    await Task.WhenAll(routedTopicRequests.Select(_ => _.SendAsync(router, cancellationToken))).ConfigureAwait(false);
                    var responses = routedTopicRequests.Select(_ => _.MetadataRetryResponse(retryAttempt, out metadataInvalid)).ToArray();
                    foreach (var response in responses.Where(_ => _.IsSuccessful)) {
                        foreach (var offsetTopic in response.Value.responses) {
                            offsets[offsetTopic.partition_id] = offsetTopic;
                        }
                    }

                    return responses.All(_ => _.IsSuccessful) 
                        ? new RetryAttempt<IImmutableList<OffsetsResponse.Topic>>(offsets.Values.ToImmutableList()) 
                        : RetryAttempt<IImmutableList<OffsetsResponse.Topic>>.Retry;
                },
                (ex, retryAttempt, retryDelay) => routedTopicRequests.MetadataRetry(ex, out metadataInvalid),
                routedTopicRequests.ThrowExtractedException, 
                cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Get offsets for all partitions of a given topic.
        /// </summary>
        /// <param name="router">The router which provides the route and metadata.</param>
        /// <param name="topicName">Name of the topic to get offset information from.</param>
        /// <param name="cancellationToken"></param>
        public static Task<IImmutableList<OffsetsResponse.Topic>> GetTopicOffsetsAsync(this IRouter router, string topicName, CancellationToken cancellationToken)
        {
            return router.GetTopicOffsetsAsync(topicName, OffsetsRequest.Topic.DefaultMaxOffsets, OffsetsRequest.Topic.LatestTime, cancellationToken);
        }

        /// <summary>
        /// Get offsets for a single partitions of a given topic.
        /// </summary>
        /// <param name="router">The router which provides the route and metadata.</param>
        /// <param name="topicName">Name of the topic to get offset information from.</param>
        /// <param name="partitionId">The partition to get offsets for.</param>
        /// <param name="maxOffsets">How many to get, at most.</param>
        /// <param name="offsetTime">These are best described by <see cref="OffsetsRequest.Topic.timestamp"/></param>
        /// <param name="cancellationToken"></param>
        public static async Task<OffsetsResponse.Topic> GetTopicOffsetAsync(this IRouter router, string topicName, int partitionId, int maxOffsets, long offsetTime, CancellationToken cancellationToken)
        {
            var request = new OffsetsRequest(new OffsetsRequest.Topic(topicName, partitionId));
            var response = await router.SendAsync(request, topicName, partitionId, cancellationToken).ConfigureAwait(false);
            return response.responses.SingleOrDefault(t => t.topic == topicName && t.partition_id == partitionId);
        }

        /// <summary>
        /// Get offsets for a single partitions of a given topic.
        /// </summary>
        public static Task<OffsetsResponse.Topic> GetTopicOffsetAsync(this IRouter router, string topicName, int partitionId, CancellationToken cancellationToken)
        {
            return router.GetTopicOffsetAsync(topicName, partitionId, OffsetsRequest.Topic.DefaultMaxOffsets, OffsetsRequest.Topic.LatestTime, cancellationToken);
        }

        /// <summary>
        /// Get offsets for a single partitions of a given topic.
        /// </summary>
        /// <param name="router">The router which provides the route and metadata.</param>
        /// <param name="topicName">Name of the topic to get offset information from.</param>
        /// <param name="partitionId">The partition to get offsets for.</param>
        /// <param name="groupId">The id of the consumer group</param>
        /// <param name="cancellationToken"></param>
        public static async Task<OffsetFetchResponse.Topic> GetTopicOffsetAsync(this IRouter router, string topicName, int partitionId, string groupId, CancellationToken cancellationToken)
        {
            var request = new OffsetFetchRequest(groupId, new TopicPartition(topicName, partitionId));
            var response = await router.SendAsync(request, topicName, partitionId, cancellationToken).ConfigureAwait(false);
            return response.responses.SingleOrDefault(t => t.topic == topicName && t.partition_id == partitionId);
        }

        #endregion
    }
}
