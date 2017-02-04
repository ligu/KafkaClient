using System;
using System.Collections.Generic;
using System.Collections.Immutable;
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
                foreach (var errorCode in response.Errors.Where(e => e != ErrorCode.None)) {
                    exceptions.Add(ExtractException(request, errorCode, endpoint));
                }
            }
            if (exceptions.Count == 0) return new RequestException(request.ApiKey, ErrorCode.None, endpoint);
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
                (errorCode == ErrorCode.UnknownMemberId ||
                errorCode == ErrorCode.IllegalGeneration || 
                errorCode == ErrorCode.InconsistentGroupProtocol))
            {
                return new MemberRequestException(member, request.ApiKey, errorCode, endpoint);
            }
            return null;
        } 

        private static FetchOutOfRangeException ExtractFetchException(FetchRequest request, ErrorCode errorCode, Endpoint endpoint)
        {
            if (errorCode == ErrorCode.OffsetOutOfRange && request?.Topics?.Count == 1) {
                return new FetchOutOfRangeException(request.Topics.First(), errorCode, endpoint);
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

        #endregion

        #region Decoding
        
        public static ErrorCode ReadErrorCode(this IKafkaReader reader)
        {
            return (ErrorCode) reader.ReadInt16();
        }

        public static bool IsSuccess(this ErrorCode code)
        {
            return code == ErrorCode.None;
        }

        /// <summary>
        /// See http://kafka.apache.org/protocol.html#protocol_error_codes for details
        /// </summary>
        public static bool IsRetryable(this ErrorCode code)
        {
            return code == ErrorCode.CorruptMessage
                || code == ErrorCode.UnknownTopicOrPartition
                || code == ErrorCode.LeaderNotAvailable
                || code == ErrorCode.NotLeaderForPartition
                || code == ErrorCode.RequestTimedOut
                || code == ErrorCode.NetworkException
                || code == ErrorCode.GroupLoadInProgress
                || code == ErrorCode.GroupCoordinatorNotAvailable
                || code == ErrorCode.NotCoordinatorForGroup
                || code == ErrorCode.NotEnoughReplicas
                || code == ErrorCode.NotEnoughReplicasAfterAppend
                || code == ErrorCode.NotController;
        }

        public static bool IsFromStaleMetadata(this ErrorCode code)
        {
            return code == ErrorCode.UnknownTopicOrPartition
                || code == ErrorCode.LeaderNotAvailable
                || code == ErrorCode.NotLeaderForPartition
                || code == ErrorCode.GroupLoadInProgress
                || code == ErrorCode.GroupCoordinatorNotAvailable
                || code == ErrorCode.NotCoordinatorForGroup;
        }

        #endregion

        #region Router

        /// <summary>
        /// Get offsets for all partitions of a given topic.
        /// </summary>
        /// <param name="router">The router which provides the route and metadata.</param>
        /// <param name="topicName">Name of the topic to get offset information from.</param>
        /// <param name="maxOffsets">How many to get, at most.</param>
        /// <param name="offsetTime">These are best described by <see cref="OffsetRequest.Topic.Timestamp"/></param>
        /// <param name="cancellationToken"></param>
        public static async Task<IImmutableList<OffsetResponse.Topic>> GetTopicOffsetsAsync(this IRouter router, string topicName, int maxOffsets, long offsetTime, CancellationToken cancellationToken)
        {
            bool? metadataInvalid = false;
            var offsets = new Dictionary<int, OffsetResponse.Topic>();
            RoutedTopicRequest<OffsetResponse>[] routedTopicRequests = null;

            return await router.Configuration.SendRetry.TryAsync(
                async (attempt, timer) => {
                    metadataInvalid = await router.RefreshTopicMetadataIfInvalidAsync(topicName, metadataInvalid, cancellationToken).ConfigureAwait(false);

                    var topicMetadata = await router.GetTopicMetadataAsync(topicName, cancellationToken).ConfigureAwait(false);
                    routedTopicRequests = topicMetadata
                        .Partitions
                        .Where(_ => !offsets.ContainsKey(_.PartitionId)) // skip partitions already successfully retrieved
                        .GroupBy(x => x.LeaderId)
                        .Select(partitions => 
                            new RoutedTopicRequest<OffsetResponse>(
                                new OffsetRequest(partitions.Select(_ => new OffsetRequest.Topic(topicName, _.PartitionId, offsetTime, maxOffsets))), 
                                topicName, 
                                partitions.Select(_ => _.PartitionId).First(), 
                                router.Log))
                        .ToArray();

                    await Task.WhenAll(routedTopicRequests.Select(_ => _.SendAsync(router, cancellationToken))).ConfigureAwait(false);
                    var responses = routedTopicRequests.Select(_ => _.MetadataRetryResponse(attempt, out metadataInvalid)).ToArray();
                    foreach (var response in responses.Where(_ => _.IsSuccessful)) {
                        foreach (var offsetTopic in response.Value.Topics) {
                            offsets[offsetTopic.PartitionId] = offsetTopic;
                        }
                    }

                    return responses.All(_ => _.IsSuccessful) 
                        ? new RetryAttempt<IImmutableList<OffsetResponse.Topic>>(offsets.Values.ToImmutableList()) 
                        : RetryAttempt<IImmutableList<OffsetResponse.Topic>>.Retry;
                },
                routedTopicRequests.MetadataRetry,
                routedTopicRequests.ThrowExtractedException,
                (ex, attempt, retry) => routedTopicRequests.MetadataRetry(attempt, ex, out metadataInvalid),
                null, // do nothing on final exception -- will be rethrown
                cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Get offsets for all partitions of a given topic.
        /// </summary>
        /// <param name="router">The router which provides the route and metadata.</param>
        /// <param name="topicName">Name of the topic to get offset information from.</param>
        /// <param name="cancellationToken"></param>
        public static Task<IImmutableList<OffsetResponse.Topic>> GetTopicOffsetsAsync(this IRouter router, string topicName, CancellationToken cancellationToken)
        {
            return router.GetTopicOffsetsAsync(topicName, OffsetRequest.Topic.DefaultMaxOffsets, OffsetRequest.Topic.LatestTime, cancellationToken);
        }

        /// <summary>
        /// Get offsets for a single partitions of a given topic.
        /// </summary>
        /// <param name="router">The router which provides the route and metadata.</param>
        /// <param name="topicName">Name of the topic to get offset information from.</param>
        /// <param name="partitionId">The partition to get offsets for.</param>
        /// <param name="maxOffsets">How many to get, at most.</param>
        /// <param name="offsetTime">These are best described by <see cref="OffsetRequest.Topic.Timestamp"/></param>
        /// <param name="cancellationToken"></param>
        public static async Task<OffsetResponse.Topic> GetTopicOffsetAsync(this IRouter router, string topicName, int partitionId, int maxOffsets, long offsetTime, CancellationToken cancellationToken)
        {
            var request = new OffsetRequest(new OffsetRequest.Topic(topicName, partitionId));
            var response = await router.SendAsync(request, topicName, partitionId, cancellationToken).ConfigureAwait(false);
            return response.Topics.SingleOrDefault(t => t.TopicName == topicName && t.PartitionId == partitionId);
        }

        /// <summary>
        /// Get offsets for a single partitions of a given topic.
        /// </summary>
        public static Task<OffsetResponse.Topic> GetTopicOffsetAsync(this IRouter router, string topicName, int partitionId, CancellationToken cancellationToken)
        {
            return router.GetTopicOffsetAsync(topicName, partitionId, OffsetRequest.Topic.DefaultMaxOffsets, OffsetRequest.Topic.LatestTime, cancellationToken);
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
            return response.Topics.SingleOrDefault(t => t.TopicName == topicName && t.PartitionId == partitionId);
        }

        #endregion
    }
}
