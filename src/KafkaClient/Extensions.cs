using System;
using System.Collections.Immutable;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Connection;
using KafkaClient.Protocol;

namespace KafkaClient
{
    public static class Extensions
    {
        /// <summary>
        /// Get offsets for each partition from a given topic.
        /// </summary>
        /// <param name="brokerRouter">The router which provides the route and metadata.</param>
        /// <param name="topicName">Name of the topic to get offset information from.</param>
        /// <param name="maxOffsets">How many to get, at most.</param>
        /// <param name="offsetTime">
        /// Used to ask for all messages before a certain time (ms). There are two special values.
        /// Specify -1 to receive the latest offsets and -2 to receive the earliest available offset.
        /// Note that because offsets are pulled in descending order, asking for the earliest offset will always return you a single element.
        /// </param>
        /// <param name="cancellationToken"></param>
        public static async Task<ImmutableList<OffsetTopic>> GetTopicOffsetAsync(this IBrokerRouter brokerRouter, string topicName, int maxOffsets, long offsetTime, CancellationToken cancellationToken)
        {
            var topicMetadata = await brokerRouter.GetTopicMetadataAsync(topicName, cancellationToken).ConfigureAwait(false);

            // send the offset request to each partition leader
            var sendRequests = topicMetadata.Partitions
                .GroupBy(x => x.PartitionId)
                .Select(p => {
                    var partitionId = p.Key;
                    var route = brokerRouter.GetBrokerRoute(topicName, partitionId);
                    var request = new OffsetRequest(new Offset(topicName, partitionId, offsetTime, maxOffsets));
                    return route.Connection.SendAsync(request, cancellationToken);
                }).ToArray();

            await Task.WhenAll(sendRequests).ConfigureAwait(false);
            return ImmutableList<OffsetTopic>.Empty.AddNotNullRange(sendRequests.SelectMany(x => x.Result.Topics));
        }

        /// <summary>
        /// Get offsets for each partition from a given topic.
        /// </summary>
        /// <param name="brokerRouter">The router which provides the route and metadata.</param>
        /// <param name="topicName">Name of the topic to get offset information from.</param>
        /// <param name="cancellationToken"></param>
        public static Task<ImmutableList<OffsetTopic>> GetTopicOffsetAsync(this IBrokerRouter brokerRouter, string topicName, CancellationToken cancellationToken)
        {
            return brokerRouter.GetTopicOffsetAsync(topicName, Offset.DefaultMaxOffsets, Offset.DefaultTime, cancellationToken);
        }

        /// <exception cref="CachedMetadataException">Thrown if the cached metadata for the given topic is invalid or missing.</exception>
        /// <exception cref="FetchOutOfRangeException">Thrown if the fetch request is not valid.</exception>
        /// <exception cref="TimeoutException">Thrown if there request times out</exception>
        /// <exception cref="ConnectionException">Thrown in case of network error contacting broker (after retries), or if none of the default brokers can be contacted.</exception>
        /// <exception cref="RequestException">Thrown in case of an unexpected error in the request</exception>
        /// <exception cref="FormatException">Thrown in case the topic name is invalid</exception>
        public static async Task<T> SendAsync<T>(this IBrokerRouter brokerRouter, IRequest<T> request, string topicName, int partition, CancellationToken cancellationToken, IRequestContext context = null, IRetry retry = null) where T : class, IResponse
        {
            if (topicName.Contains(" ")) throw new FormatException($"topic name ({topicName}) is invalid");

            bool? metadataInvalid = false;
            return await (retry ?? new Retry(TimeSpan.MaxValue, 3)).AttemptAsync(
                async (attempt, timer) => {
                    if (metadataInvalid.GetValueOrDefault(true)) {
                        // unknown metadata status should not force the issue
                        await brokerRouter.RefreshTopicMetadataAsync(topicName, metadataInvalid.GetValueOrDefault(), cancellationToken).ConfigureAwait(false);
                        metadataInvalid = false;
                    }

                    brokerRouter.Log.DebugFormat("Router SendAsync request {0} (attempt {1})", request.ApiKey, attempt + 1);
                    var route = await brokerRouter.GetBrokerRouteAsync(topicName, partition, cancellationToken);
                    var endpoint = route.Connection.Endpoint;
                    var response = await route.Connection.SendAsync(request, cancellationToken, context).ConfigureAwait(false);

                    // this can happen if you send ProduceRequest with ack level=0
                    if (response == null) return null;

                    var errors = response.Errors.Where(e => e != ErrorResponseCode.NoError).ToList();
                    if (errors.Count == 0) return response;

                    metadataInvalid = errors.All(IsRecoverableByMetadaRefresh);
                    brokerRouter.Log.WarnFormat("Error response in Router SendAsync (attempt {0}): {1}", 
                        attempt + 1, errors.Aggregate($"{route} - ", (buffer, e) => $"{buffer} {e}"));

                    if (!metadataInvalid.Value) throw request.ExtractExceptions(response, endpoint);
                    throw new SendException<T>(request, response, endpoint);
                },
                (ex, attempt, retryDelay) => {
                    var metadataPotentiallyInvalid = IsPotentiallyRecoverableByMetadataRefresh(ex);
                    var sendException = ex as SendException<T>;

                    if (!retryDelay.HasValue && sendException != null) {
                        throw sendException.Request.ExtractExceptions(sendException.Response, sendException.Endpoint);
                    }
                    if (!retryDelay.HasValue || (sendException == null && !metadataPotentiallyInvalid)) {
                        var exceptionInfo = ExceptionDispatchInfo.Capture(ex);
                        exceptionInfo.Throw();
                    }

                    if (metadataPotentiallyInvalid) {
                        metadataInvalid = null; // ie. the state of the metadata is unknown
                    }
                },
                cancellationToken);
        }

        private class SendException<TResponse> : Exception
            where TResponse : class, IResponse
        {
            public SendException(IRequest<TResponse> request, TResponse response, Endpoint endpoint)
            {
                Request = request;
                Response = response;
                Endpoint = endpoint;
            }

            public IRequest<TResponse> Request { get; }
            public TResponse Response { get; }
            public Endpoint Endpoint { get; }
        }

        private static bool IsPotentiallyRecoverableByMetadataRefresh(Exception exception)
        {
            return exception is FetchOutOfRangeException
                || exception is TimeoutException
                || exception is ConnectionException
                || exception is CachedMetadataException;
        }

        private static bool IsRecoverableByMetadaRefresh(ErrorResponseCode error)
        {
            return  error == ErrorResponseCode.BrokerNotAvailable ||
                    error == ErrorResponseCode.ConsumerCoordinatorNotAvailable ||
                    error == ErrorResponseCode.LeaderNotAvailable ||
                    error == ErrorResponseCode.NotLeaderForPartition;
        }
    }
}