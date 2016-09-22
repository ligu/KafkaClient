using System;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Connection;
using KafkaClient.Protocol;

namespace KafkaClient
{
    public class ProtocolGateway : IDisposable
    {
        private readonly IBrokerRouter _brokerRouter;

        //Add Loger
        public ProtocolGateway(params Uri[] brokerUrl)
        {
            var kafkaOptions = new KafkaOptions(brokerUrl) { MaximumReconnectionTimeout = TimeSpan.FromSeconds(60), ResponseTimeoutMs = TimeSpan.FromSeconds(60) };
            _brokerRouter = new BrokerRouter(kafkaOptions);
        }

        public ProtocolGateway(IBrokerRouter brokerRouter)
        {
            _brokerRouter = brokerRouter;
        }

        public ProtocolGateway(KafkaOptions kafkaOptions)
        {
            _brokerRouter = new BrokerRouter(kafkaOptions);
        }

        private readonly int _maxRetry = 3;

        /// <exception cref="CachedMetadataException">Thrown if the cached metadata for the given topic is invalid or missing.</exception>
        /// <exception cref="FetchOutOfRangeException">Thrown if the fetch request is not valid.</exception>
        /// <exception cref="TimeoutException">Thrown if there request times out</exception>
        /// <exception cref="KafkaConnectionException">Thrown in case of network error contacting broker (after retries), or if none of the default brokers can be contacted.</exception>
        /// <exception cref="KafkaRequestException">Thrown in case of an unexpected error in the request</exception>
        /// <exception cref="FormatException">Thrown in case the topic name is invalid</exception>
        public async Task<T> SendProtocolRequestAsync<T>(IKafkaRequest<T> request, string topic, int partition, CancellationToken cancellationToken, IRequestContext context = null) where T : class, IKafkaResponse
        {
            if (topic.Contains(" ")) throw new FormatException($"topic name ({topic}) is invalid");

            T response = null;
            var retryTime = 0;
            IKafkaConnection connection = null;
            while (retryTime < _maxRetry) {
                var metadataPotentiallyInvalid = false;
                var metadataKnownInvalid = false;
                ExceptionDispatchInfo exceptionInfo = null;
                var errorDetails = "";

                try {
                    var route = await _brokerRouter.GetBrokerRouteAsync(topic, partition, cancellationToken);
                    connection = route.Connection;
                    response = await route.Connection.SendAsync(request, cancellationToken, context).ConfigureAwait(false);

                    if (response == null) {
                        // this can happen if you send ProduceRequest with ack level=0
                        return null;
                    }

                    var errors = response.Errors.Where(e => e != ErrorResponseCode.NoError).ToList();
                    if (errors.Count == 0) {
                        return response;
                    }

                    errorDetails = errors.Aggregate(new StringBuilder($"{route} - "), (buffer, e) => buffer.Append(" ").Append(e)).ToString();
                    metadataKnownInvalid = errors.All(CanRecoverByRefreshMetadata);
                } catch (TimeoutException ex) {
                    exceptionInfo = ExceptionDispatchInfo.Capture(ex);
                } catch (KafkaConnectionException ex) {
                    exceptionInfo = ExceptionDispatchInfo.Capture(ex);
                } catch (FetchOutOfRangeException ex) {
                    exceptionInfo = ExceptionDispatchInfo.Capture(ex);
                } catch (CachedMetadataException ex) {
                    exceptionInfo = ExceptionDispatchInfo.Capture(ex);
                }

                if (exceptionInfo != null) {
                    metadataPotentiallyInvalid = true;
                    errorDetails = exceptionInfo.SourceException.GetType().Name;
                }

                retryTime++;
                var hasMoreRetry = retryTime < _maxRetry;

                _brokerRouter.Log.WarnFormat("ProtocolGateway error sending request, retrying (attempt number {0}): {1}", retryTime, errorDetails);
                if ((metadataKnownInvalid || metadataPotentiallyInvalid) && hasMoreRetry) {
                    await _brokerRouter.RefreshTopicMetadataAsync(topic, metadataKnownInvalid, cancellationToken).ConfigureAwait(false);
                } else {
                    _brokerRouter.Log.ErrorFormat("ProtocolGateway sending request failed");

                    // If an exception was thrown, we want to propagate it
                    exceptionInfo?.Throw();

                    // Otherwise, the error was from Kafka, throwing application exception
                    throw request.ExtractExceptions(response, connection?.Endpoint);
                }
            }

            return response;
        }

        private static bool CanRecoverByRefreshMetadata(ErrorResponseCode error)
        {
            return  error == ErrorResponseCode.BrokerNotAvailable ||
                    error == ErrorResponseCode.ConsumerCoordinatorNotAvailable ||
                    error == ErrorResponseCode.LeaderNotAvailable ||
                    error == ErrorResponseCode.NotLeaderForPartition;
        }

        public void Dispose()
        {
            _brokerRouter.Dispose();
        }
    }
}