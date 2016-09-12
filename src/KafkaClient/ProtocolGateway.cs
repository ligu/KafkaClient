using KafkaNet.Model;
using KafkaNet.Protocol;
using System;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace KafkaNet
{
    public static class Extensions
    {
        public static T GetValue<T>(this SerializationInfo info, string name)
        {
            return (T)info.GetValue(name, typeof(T));
        }
    }

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
        public async Task<T> SendProtocolRequest<T>(IKafkaRequest<T> request, string topic, int partition, IRequestContext context = null) where T : class, IKafkaResponse
        {
            ValidateTopic(topic);
            T response = null;
            var retryTime = 0;
            IKafkaConnection connection = null;
            while (retryTime < _maxRetry) {
                var needToRefreshTopicMetadata = false;
                ExceptionDispatchInfo exceptionInfo = null;
                var errorDetails = "";

                try {
                    await _brokerRouter.RefreshMissingTopicMetadata(topic);

                    // route can change after Metadata Refresh
                    var route = _brokerRouter.SelectBrokerRouteFromLocalCache(topic, partition);
                    connection = route.Connection;
                    response = await route.Connection.SendAsync(request, context).ConfigureAwait(false);

                    if (response == null) {
                        // this can happen if you send ProduceRequest with ack level=0
                        return null;
                    }

                    var errors = response.Errors.Where(e => e != ErrorResponseCode.NoError).ToList();
                    if (errors.Count == 0) {
                        return response;
                    }

                    errorDetails = errors.Aggregate(new StringBuilder($"{route} - "), (buffer, e) => buffer.Append(" ").Append(e)).ToString();
                    needToRefreshTopicMetadata = errors.All(CanRecoverByRefreshMetadata);
                } catch (TimeoutException ex) {
                    // TODO: wrap this in another exception type?
                    exceptionInfo = ExceptionDispatchInfo.Capture(ex);
                } catch (KafkaConnectionException ex) {
                    exceptionInfo = ExceptionDispatchInfo.Capture(ex);
                } catch (FetchOutOfRangeException ex) {
                    exceptionInfo = ExceptionDispatchInfo.Capture(ex);
                } catch (CachedMetadataException ex) {
                    exceptionInfo = ExceptionDispatchInfo.Capture(ex);
                }

                if (exceptionInfo != null) {
                    needToRefreshTopicMetadata = true;
                    errorDetails = exceptionInfo.SourceException.GetType().Name;
                }

                retryTime++;
                var hasMoreRetry = retryTime < _maxRetry;

                _brokerRouter.Log.WarnFormat("ProtocolGateway error sending request, retrying (attempt number {0}): {1}", retryTime, errorDetails);
                if (needToRefreshTopicMetadata && hasMoreRetry) {
                    await _brokerRouter.RefreshTopicMetadata(topic).ConfigureAwait(false);
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

        private void ValidateTopic(string topic)
        {
            if (topic.Contains(" ")) {
                throw new FormatException("topic name is invalid");
            }
        }
    }
}