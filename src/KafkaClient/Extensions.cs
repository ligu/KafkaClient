using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
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
        public static async Task<List<OffsetTopic>> GetTopicOffsetAsync(this IBrokerRouter brokerRouter, string topicName, int maxOffsets, long offsetTime, CancellationToken cancellationToken)
        {
            var topicMetadata = await brokerRouter.GetTopicMetadataAsync(topicName, cancellationToken).ConfigureAwait(false);

            // send the offset request to each partition leader
            var sendRequests = topicMetadata.Partitions
                .GroupBy(x => x.PartitionId)
                .Select(p =>
                    {
                        var route = brokerRouter.GetBrokerRoute(topicName, p.Key);
                        var request = new OffsetRequest(new Offset(topicName, p.Key, offsetTime, maxOffsets));

                        return route.Connection.SendAsync(request, cancellationToken);
                    }).ToArray();

            await Task.WhenAll(sendRequests).ConfigureAwait(false);
            return sendRequests.SelectMany(x => x.Result.Topics).ToList();
        }

        /// <exception cref="CachedMetadataException">Thrown if the cached metadata for the given topic is invalid or missing.</exception>
        /// <exception cref="FetchOutOfRangeException">Thrown if the fetch request is not valid.</exception>
        /// <exception cref="TimeoutException">Thrown if there request times out</exception>
        /// <exception cref="KafkaConnectionException">Thrown in case of network error contacting broker (after retries), or if none of the default brokers can be contacted.</exception>
        /// <exception cref="KafkaRequestException">Thrown in case of an unexpected error in the request</exception>
        /// <exception cref="FormatException">Thrown in case the topic name is invalid</exception>
        public static async Task<T> SendAsync<T>(this IBrokerRouter brokerRouter, IKafkaRequest<T> request, string topicName, int partition, CancellationToken cancellationToken, IRequestContext context = null) where T : class, IKafkaResponse
        {
            if (topicName.Contains(" ")) throw new FormatException($"topic name ({topicName}) is invalid");

            ExceptionDispatchInfo exceptionInfo = null;
            KafkaEndpoint endpoint = null;
            T response = null;
            bool? metadataInvalid = false;
            var attempt = 1;
            do {
                if (metadataInvalid.GetValueOrDefault(true)) {
                    // unknown metadata status should not force the issue
                    await brokerRouter.RefreshTopicMetadataAsync(topicName, metadataInvalid.GetValueOrDefault(), cancellationToken).ConfigureAwait(false);
                }

                try {
                    brokerRouter.Log.DebugFormat("Router SendAsync request {0} (attempt {1})", request.ApiKey, attempt + 1);
                    var route = await brokerRouter.GetBrokerRouteAsync(topicName, partition, cancellationToken);
                    endpoint = route.Connection.Endpoint;
                    response = await route.Connection.SendAsync(request, cancellationToken, context).ConfigureAwait(false);

                    // this can happen if you send ProduceRequest with ack level=0
                    if (response == null) return null;

                    var errors = response.Errors.Where(e => e != ErrorResponseCode.NoError).ToList();
                    if (errors.Count == 0) return response;

                    metadataInvalid = errors.All(CanRecoverByRefreshMetadata);
                    brokerRouter.Log.WarnFormat("Error response in Router SendAsync (attempt {0}): {1}", 
                        attempt + 1, errors.Aggregate($"{route} - ", (buffer, e) => $"{buffer} {e}"));
                } catch (Exception ex) {
                    if (!(ex is TimeoutException || ex is KafkaConnectionException || ex is FetchOutOfRangeException || ex is CachedMetadataException)) throw;

                    exceptionInfo = ExceptionDispatchInfo.Capture(ex);
                    metadataInvalid = null; // ie. the state of the metadata is unknown
                    brokerRouter.Log.WarnFormat(ex, "Error response in Router SendAsync (attempt {0})", attempt + 1);
                }
            } while (attempt++ < 3 && metadataInvalid.GetValueOrDefault(true));

            brokerRouter.Log.ErrorFormat("Router SendAsync Failed");
            exceptionInfo?.Throw();
            throw request.ExtractExceptions(response, endpoint);
        }

        private static bool CanRecoverByRefreshMetadata(ErrorResponseCode error)
        {
            return  error == ErrorResponseCode.BrokerNotAvailable ||
                    error == ErrorResponseCode.ConsumerCoordinatorNotAvailable ||
                    error == ErrorResponseCode.LeaderNotAvailable ||
                    error == ErrorResponseCode.NotLeaderForPartition;
        }
    }
}