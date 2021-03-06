﻿using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Connections;
using KafkaClient.Protocol;

namespace KafkaClient
{
    public static class BrokerRouterExtensions
    {
        /// <summary>
        /// Get offsets for all partitions of a given topic.
        /// </summary>
        /// <param name="brokerRouter">The router which provides the route and metadata.</param>
        /// <param name="topicName">Name of the topic to get offset information from.</param>
        /// <param name="maxOffsets">How many to get, at most.</param>
        /// <param name="offsetTime">These are best described by <see cref="OffsetRequest.Topic.Timestamp"/></param>
        /// <param name="cancellationToken"></param>
        /// <param name="retryPolicy"></param>
        public static async Task<IImmutableList<OffsetResponse.Topic>> GetTopicOffsetsAsync(this IBrokerRouter brokerRouter, string topicName, int maxOffsets, long offsetTime, CancellationToken cancellationToken, IRetry retryPolicy = null)
        {
            bool? metadataInvalid = false;
            var offsets = new Dictionary<int, OffsetResponse.Topic>();
            BrokeredRequest<OffsetResponse>[] brokeredRequests = null;

            return await (retryPolicy ?? new Retry(TimeSpan.MaxValue, 3)).AttemptAsync(
                async (attempt, timer) => {
                    metadataInvalid = await brokerRouter.RefreshTopicMetadataIfInvalidAsync(topicName, metadataInvalid, cancellationToken).ConfigureAwait(false);

                    var topicMetadata = await brokerRouter.GetTopicMetadataAsync(topicName, cancellationToken).ConfigureAwait(false);
                    brokeredRequests = topicMetadata
                        .Partitions
                        .Where(_ => !offsets.ContainsKey(_.PartitionId)) // skip partitions already successfully retrieved
                        .GroupBy(x => x.LeaderId)
                        .Select(partitions => 
                            new BrokeredRequest<OffsetResponse>(
                                new OffsetRequest(partitions.Select(_ => new OffsetRequest.Topic(topicName, _.PartitionId, offsetTime, maxOffsets))), 
                                topicName, 
                                partitions.Select(_ => _.PartitionId).First(), 
                                brokerRouter.Log))
                        .ToArray();

                    await Task.WhenAll(brokeredRequests.Select(_ => _.SendAsync(brokerRouter, cancellationToken))).ConfigureAwait(false);
                    var responses = brokeredRequests.Select(_ => _.MetadataRetryResponse(attempt, out metadataInvalid)).ToArray();
                    foreach (var response in responses.Where(_ => _.IsSuccessful)) {
                        foreach (var offsetTopic in response.Value.Topics) {
                            offsets[offsetTopic.PartitionId] = offsetTopic;
                        }
                    }

                    return responses.All(_ => _.IsSuccessful) 
                        ? new RetryAttempt<IImmutableList<OffsetResponse.Topic>>(offsets.Values.ToImmutableList()) 
                        : RetryAttempt<IImmutableList<OffsetResponse.Topic>>.Retry;
                },
                brokeredRequests.MetadataRetry,
                brokeredRequests.ThrowExtractedException,
                (ex, attempt, retry) => brokeredRequests.MetadataRetry(attempt, ex, out metadataInvalid),
                null, // do nothing on final exception -- will be rethrown
                cancellationToken);
        }

        /// <summary>
        /// Get offsets for all partitions of a given topic.
        /// </summary>
        /// <param name="brokerRouter">The router which provides the route and metadata.</param>
        /// <param name="topicName">Name of the topic to get offset information from.</param>
        /// <param name="cancellationToken"></param>
        public static Task<IImmutableList<OffsetResponse.Topic>> GetTopicOffsetsAsync(this IBrokerRouter brokerRouter, string topicName, CancellationToken cancellationToken)
        {
            return brokerRouter.GetTopicOffsetsAsync(topicName, OffsetRequest.Topic.DefaultMaxOffsets, OffsetRequest.Topic.LatestTime, cancellationToken);
        }

        /// <summary>
        /// Get offsets for a single partitions of a given topic.
        /// </summary>
        /// <param name="brokerRouter">The router which provides the route and metadata.</param>
        /// <param name="topicName">Name of the topic to get offset information from.</param>
        /// <param name="partitionId">The partition to get offsets for.</param>
        /// <param name="maxOffsets">How many to get, at most.</param>
        /// <param name="offsetTime">These are best described by <see cref="OffsetRequest.Topic.Timestamp"/></param>
        /// <param name="cancellationToken"></param>
        public static async Task<OffsetResponse.Topic> GetTopicOffsetAsync(this IBrokerRouter brokerRouter, string topicName, int partitionId, int maxOffsets, long offsetTime, CancellationToken cancellationToken)
        {
            var request = new OffsetRequest(new OffsetRequest.Topic(topicName, partitionId));
            var response = await brokerRouter.SendAsync(request, topicName, partitionId, cancellationToken).ConfigureAwait(false);
            return response.Topics.SingleOrDefault(t => t.TopicName == topicName && t.PartitionId == partitionId);
        }

        /// <summary>
        /// Get offsets for a single partitions of a given topic.
        /// </summary>
        public static Task<OffsetResponse.Topic> GetTopicOffsetAsync(this IBrokerRouter brokerRouter, string topicName, int partitionId, CancellationToken cancellationToken)
        {
            return brokerRouter.GetTopicOffsetAsync(topicName, partitionId, OffsetRequest.Topic.DefaultMaxOffsets, OffsetRequest.Topic.LatestTime, cancellationToken);
        }

        /// <summary>
        /// Get offsets for a single partitions of a given topic.
        /// </summary>
        /// <param name="brokerRouter">The router which provides the route and metadata.</param>
        /// <param name="topicName">Name of the topic to get offset information from.</param>
        /// <param name="partitionId">The partition to get offsets for.</param>
        /// <param name="consumerGroup">The id of the consumer group</param>
        /// <param name="cancellationToken"></param>
        public static async Task<OffsetFetchResponse.Topic> GetTopicOffsetAsync(this IBrokerRouter brokerRouter, string topicName, int partitionId, string consumerGroup, CancellationToken cancellationToken)
        {
            var request = new OffsetFetchRequest(consumerGroup, new TopicPartition(topicName, partitionId));
            var response = await brokerRouter.SendAsync(request, topicName, partitionId, consumerGroup, cancellationToken).ConfigureAwait(false);
            return response.Topics.SingleOrDefault(t => t.TopicName == topicName && t.PartitionId == partitionId);
        }

        /// <summary>
        /// Get offsets for a single partitions of a given topic.
        /// </summary>
        /// <param name="brokerRouter">The router which provides the route and metadata.</param>
        /// <param name="topicName">Name of the topic to get offset information from.</param>
        /// <param name="partitionId">The partition to get offsets for.</param>
        /// <param name="consumerGroup">The id of the consumer group</param>
        /// <param name="offset">The new offset</param>
        /// <param name="cancellationToken"></param>
        public static async Task CommitTopicOffsetAsync(this IBrokerRouter brokerRouter, string topicName, int partitionId, string consumerGroup, long offset, CancellationToken cancellationToken)
        {
            var request = new OffsetCommitRequest(consumerGroup, new [] { new OffsetCommitRequest.Topic(topicName, partitionId, offset) });
            await brokerRouter.SendAsync(request, topicName, partitionId, consumerGroup, cancellationToken).ConfigureAwait(false);
        }

        private static async Task<T> SendAsync<T>(this IBrokerRouter brokerRouter, IRequest<T> request, string topicName, int partitionId, string consumerGroup, CancellationToken cancellationToken) where T : class, IResponse
        {
            try {
                return await brokerRouter.SendAsync(request, topicName, partitionId, cancellationToken).ConfigureAwait(false);
            } catch (RequestException ex) when (ex.ErrorCode == ErrorResponseCode.NotCoordinatorForGroup) {
                // ensure the group exists, then retry
                await brokerRouter.SendAsync(new GroupCoordinatorRequest(consumerGroup), topicName, partitionId, cancellationToken).ConfigureAwait(false);
                return await brokerRouter.SendAsync(request, topicName, partitionId, cancellationToken).ConfigureAwait(false);
            }
        }

        /// <exception cref="CachedMetadataException">Thrown if the cached metadata for the given topic is invalid or missing.</exception>
        /// <exception cref="FetchOutOfRangeException">Thrown if the fetch request is not valid.</exception>
        /// <exception cref="TimeoutException">Thrown if there request times out</exception>
        /// <exception cref="ConnectionException">Thrown in case of network error contacting broker (after retries), or if none of the default brokers can be contacted.</exception>
        /// <exception cref="RequestException">Thrown in case of an unexpected error in the request</exception>
        public static async Task<T> SendAsync<T>(this IBrokerRouter brokerRouter, IRequest<T> request, string topicName, int partitionId, CancellationToken cancellationToken, IRequestContext context = null, IRetry retryPolicy = null) where T : class, IResponse
        {
            bool? metadataInvalid = false;
            var brokeredRequest = new BrokeredRequest<T>(request, topicName, partitionId, brokerRouter.Log);

            return await (retryPolicy ?? new Retry(TimeSpan.MaxValue, 3)).AttemptAsync(
                async (attempt, timer) => {
                    metadataInvalid = await brokerRouter.RefreshTopicMetadataIfInvalidAsync(topicName, metadataInvalid, cancellationToken).ConfigureAwait(false);
                    await brokeredRequest.SendAsync(brokerRouter, cancellationToken, context).ConfigureAwait(false);
                    return brokeredRequest.MetadataRetryResponse(attempt, out metadataInvalid);
                },
                brokeredRequest.MetadataRetry,
                brokeredRequest.ThrowExtractedException,
                (ex, attempt, retry) => brokeredRequest.MetadataRetry(attempt, ex, out metadataInvalid),
                null, // do nothing on final exception -- will be rethrown
                cancellationToken);
        }

        public static async Task<bool> RefreshTopicMetadataIfInvalidAsync(this IBrokerRouter brokerRouter, string topicName, bool? metadataInvalid, CancellationToken cancellationToken)
        {
            if (metadataInvalid.GetValueOrDefault(true)) {
                // unknown metadata status should not force the issue
                await brokerRouter.RefreshTopicMetadataAsync(topicName, metadataInvalid.GetValueOrDefault(), cancellationToken).ConfigureAwait(false);
            }
            return false;
        }

        public static async Task<bool> RefreshTopicMetadataIfInvalidAsync(this IBrokerRouter brokerRouter, IEnumerable<string> topicNames, bool? metadataInvalid, CancellationToken cancellationToken)
        {
            if (metadataInvalid.GetValueOrDefault(true)) {
                // unknown metadata status should not force the issue
                await brokerRouter.RefreshTopicMetadataAsync(topicNames, metadataInvalid.GetValueOrDefault(), cancellationToken).ConfigureAwait(false);
            }
            return false;
        }

        internal static void MetadataRetry<T>(this IEnumerable<BrokeredRequest<T>> brokeredRequests, int attempt, TimeSpan retry) where T : class, IResponse
        {
            foreach (var brokeredRequest in brokeredRequests) {
                brokeredRequest.MetadataRetry(attempt, retry);
            }
        }

        internal static void ThrowExtractedException<T>(this BrokeredRequest<T>[] brokeredRequests, int attempt) where T : class, IResponse
        {
            throw brokeredRequests.Select(_ => _.ResponseException).FlattenAggregates();
        }

        internal static void MetadataRetry<T>(this IEnumerable<BrokeredRequest<T>> brokeredRequests, int attempt, Exception exception, out bool? retry) where T : class, IResponse
        {
            retry = null;
            foreach (var brokeredRequest in brokeredRequests) {
                bool? requestRetry;
                brokeredRequest.MetadataRetry(attempt, exception, out requestRetry);
                if (requestRetry.HasValue) {
                    retry = requestRetry;
                }
            }
        }

        internal static bool IsPotentiallyRecoverableByMetadataRefresh(this Exception exception)
        {
            return exception is FetchOutOfRangeException
                || exception is TimeoutException
                || exception is ConnectionException
                || exception is CachedMetadataException;
        }

        /// <summary>
        /// Given a collection of server connections, query for the topic metadata.
        /// </summary>
        /// <param name="brokerRouter">The router which provides the route and metadata.</param>
        /// <param name="topicNames">Topics to get metadata information for.</param>
        /// <param name="cancellationToken"></param>
        /// <remarks>
        /// Used by <see cref="BrokerRouter"/> internally. Broken out for better testability, but not intended to be used separately.
        /// </remarks>
        /// <returns>MetadataResponse validated to be complete.</returns>
        internal static async Task<MetadataResponse> GetMetadataAsync(this IBrokerRouter brokerRouter, IEnumerable<string> topicNames, CancellationToken cancellationToken)
        {
            var request = new MetadataRequest(topicNames);

            return await brokerRouter.Configuration.RefreshRetry.AttemptAsync(
                async (attempt, timer) => {
                    var response = await brokerRouter.GetMetadataAsync(request, cancellationToken).ConfigureAwait(false);
                    if (response == null) return new RetryAttempt<MetadataResponse>(null);

                    var results = response.Brokers
                        .Select(ValidateBroker)
                        .Union(response.Topics.Select(ValidateTopic))
                        .Where(r => !r.IsValid.GetValueOrDefault())
                        .ToList();

                    var exceptions = results.Select(r => r.ToException()).Where(e => e != null).ToList();
                    if (exceptions.Count == 1) throw exceptions.Single();
                    if (exceptions.Count > 1) throw new AggregateException(exceptions);

                    if (results.Count == 0) return new RetryAttempt<MetadataResponse>(response);
                    foreach (var result in results.Where(r => !string.IsNullOrEmpty(r.Message))) {
                        brokerRouter.Log.Warn(() => LogEvent.Create(result.Message));
                    }

                    return RetryAttempt<MetadataResponse>.Retry;
                },
                (attempt, retry) => brokerRouter.Log.Warn(() => LogEvent.Create($"Failed metadata request on attempt {attempt}: Will retry in {retry}")),
                null, // return the failed response above, resulting in a null
                (ex, attempt, retry) => {
                    throw ex.PrepareForRethrow();
                },
                (ex, attempt) => brokerRouter.Log.Warn(() => LogEvent.Create(ex, $"Failed metadata request on attempt {attempt}")),
                cancellationToken);
        }

        private static async Task<MetadataResponse> GetMetadataAsync(this IBrokerRouter brokerRouter, MetadataRequest request, CancellationToken cancellationToken)
        {
            var servers = new List<string>();
            foreach (var connection in brokerRouter.Connections) {
                var server = connection.Endpoint?.ToString();
                try {
                    return await connection.SendAsync(request, cancellationToken).ConfigureAwait(false);
                } catch (Exception ex) {
                    servers.Add(server);
                    brokerRouter.Log.Warn(() => LogEvent.Create(ex, $"Failed to contact {server}: Trying next server"));
                }
            }

            throw new RequestException(request.ApiKey, ErrorResponseCode.None, $"Unable to make Metadata Request to any of {string.Join(" ", servers)}");
        }

        private class MetadataResult
        {
            public bool? IsValid { get; }
            public string Message { get; }
            private readonly ErrorResponseCode _errorCode;

            public Exception ToException()
            {
                if (IsValid.GetValueOrDefault(true)) return null;

                if (_errorCode == ErrorResponseCode.None) return new ConnectionException(Message);
                return new RequestException(ApiKeyRequestType.Metadata, _errorCode, Message);
            }

            public MetadataResult(ErrorResponseCode errorCode = ErrorResponseCode.None, bool? isValid = null, string message = null)
            {
                Message = message ?? "";
                _errorCode = errorCode;
                IsValid = isValid;
            }
        }

        private static MetadataResult ValidateBroker(Broker broker)
        {
            if (broker.BrokerId == -1)             return new MetadataResult(ErrorResponseCode.Unknown);
            if (string.IsNullOrEmpty(broker.Host)) return new MetadataResult(ErrorResponseCode.None, false, "Broker missing host information.");
            if (broker.Port <= 0)                  return new MetadataResult(ErrorResponseCode.None, false, "Broker missing port information.");
            return new MetadataResult(isValid: true);
        }

        private static MetadataResult ValidateTopic(MetadataResponse.Topic topic)
        {
            var errorCode = topic.ErrorCode;
            if (errorCode == ErrorResponseCode.None) return new MetadataResult(isValid: true);
            if (errorCode.IsRetryable()) return new MetadataResult(errorCode, null, $"topic/{topic.TopicName} returned error code of {errorCode}: Retrying");
            return new MetadataResult(errorCode, false, $"topic/{topic.TopicName} returned an error of {errorCode}");
        }
    }
}