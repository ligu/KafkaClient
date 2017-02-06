using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Assignment;
using KafkaClient.Common;
using KafkaClient.Connections;
using KafkaClient.Protocol;
using KafkaClient.Telemetry;

namespace KafkaClient
{
    public static class Extensions
    {
        #region Configuration helpers

        public static IVersionSupport Dynamic(this VersionSupport versionSupport)
        {
            return new DynamicVersionSupport(versionSupport);
        }

        public static IConnectionConfiguration ToConfiguration(this ITrackEvents tracker)
        {
            if (tracker == null) return new ConnectionConfiguration();

            return new ConnectionConfiguration(
                onDisconnected: tracker.Disconnected,
                onConnecting: tracker.Connecting,
                onConnected: tracker.Connected,
                onWriting: tracker.Writing,
                onWritingBytes: tracker.WritingBytes,
                onWroteBytes: tracker.WroteBytes,
                onWritten: tracker.Written,
                onWriteFailed: tracker.WriteFailed,
                onReading: tracker.Reading,
                onReadingBytes: tracker.ReadingBytes,
                onReadBytes: tracker.ReadBytes,
                onRead: tracker.Read,
                onReadFailed: tracker.ReadFailed,
                onProduceRequestMessages: tracker.ProduceRequestMessages);
        }

        public static IConnectionConfiguration CopyWith(
            this IConnectionConfiguration configuration,
            IRetry connectionRetry = null,
            IVersionSupport versionSupport = null,
            TimeSpan? requestTimeout = null,
            int? readBufferSize = null,
            int? writeBufferSize = null,
            bool? isTcpKeepalive = null,
            IEnumerable<IMembershipEncoder> encoders = null,
            ISslConfiguration sslConfiguration = null,
            ConnectError onDisconnected = null,
            Connecting onConnecting = null,
            Connecting onConnected = null,
            Writing onWriting = null,
            StartingBytes onWritingBytes = null,
            FinishedBytes onWroteBytes = null,
            WriteSuccess onWritten = null,
            WriteError onWriteFailed = null,
            Reading onReading = null,
            StartingBytes onReadingBytes = null,
            FinishedBytes onReadBytes = null,
            ReadSuccess onRead = null,
            ReadError onReadFailed = null,
            ProduceRequestMessages onProduceRequestMessages = null)
        {
            return new ConnectionConfiguration(
                connectionRetry ?? configuration.ConnectionRetry,
                versionSupport ?? configuration.VersionSupport,
                requestTimeout ?? configuration.RequestTimeout,
                readBufferSize ?? configuration.ReadBufferSize,
                writeBufferSize ?? configuration.WriteBufferSize,
                isTcpKeepalive ?? configuration.IsTcpKeepalive,
                encoders ?? configuration.Encoders.Values,
                sslConfiguration ?? configuration.SslConfiguration,
                onDisconnected ?? configuration.OnDisconnected,
                onConnecting ?? configuration.OnConnecting,
                onConnected ?? configuration.OnConnected,
                onWriting ?? configuration.OnWriting,
                onWritingBytes ?? configuration.OnWritingBytes,
                onWroteBytes ?? configuration.OnWroteBytes,
                onWritten ?? configuration.OnWritten,
                onWriteFailed ?? configuration.OnWriteFailed,
                onReading ?? configuration.OnReading,
                onReadingBytes ?? configuration.OnReadingBytes,
                onReadBytes ?? configuration.OnReadBytes,
                onRead ?? configuration.OnRead,
                onReadFailed ?? configuration.OnReadFailed,
                onProduceRequestMessages ?? configuration.OnProduceRequestMessages);
        }

        #endregion

        #region KafkaOptions

        public static async Task<IConnection> CreateConnectionAsync(this KafkaOptions options)
        {
            var endpoint = await Endpoint.ResolveAsync(options.ServerUris.First(), options.Log);
            return options.CreateConnection(endpoint);
        }

        public static IConnection CreateConnection(this KafkaOptions options, Endpoint endpoint)
        {
            return options.ConnectionFactory.Create(endpoint, options.ConnectionConfiguration, options.Log);
        }

        public static async Task<IConsumer> CreateConsumerAsync(this KafkaOptions options)
        {
            return new Consumer(await options.CreateRouterAsync(), options.ConsumerConfiguration, options.ConnectionConfiguration.Encoders, false);
        }

        public static async Task<IProducer> CreateProducerAsync(this KafkaOptions options)
        {
            return new Producer(await options.CreateRouterAsync(), options.ProducerConfiguration, false);
        }

        public static Task<Router> CreateRouterAsync(this KafkaOptions options)
        {
            return Router.CreateAsync(
                options.ServerUris,
                options.ConnectionFactory,
                options.ConnectionConfiguration,
                options.RouterConfiguration,
                options.Log);
        }

        #endregion

        #region Producing

        /// <summary>
        /// Send a message to the given topic.
        /// </summary>
        /// <param name="producer">The message producer</param>
        /// <param name="messages">The messages to send.</param>
        /// <param name="topicName">The name of the kafka topic to send the messages to.</param>
        /// <param name="partitionId">The partition to send messages to</param>
        /// <param name="cancellationToken"></param>
        /// <returns>List of ProduceTopic response from each partition sent to or empty list if acks = 0.</returns>
        public static Task<ProduceResponse.Topic> SendMessagesAsync(this IProducer producer, IEnumerable<Message> messages, string topicName, int partitionId, CancellationToken cancellationToken)
        {
            return producer.SendMessagesAsync(messages, topicName, partitionId, null, cancellationToken);
        }

        /// <summary>
        /// Send a message to the given topic.
        /// </summary>
        /// <param name="producer">The message producer</param>
        /// <param name="messages">The messages to send.</param>
        /// <param name="topicName">The name of the kafka topic to send the messages to.</param>
        /// <param name="cancellationToken"></param>
        /// <returns>List of ProduceTopic response from each partition sent to or empty list if acks = 0.</returns>
        public static Task<IEnumerable<ProduceResponse.Topic>> SendMessagesAsync(this IProducer producer, IEnumerable<Message> messages, string topicName, CancellationToken cancellationToken)
        {
            return producer.SendMessagesAsync(messages, topicName, null, cancellationToken);
        }

        /// <summary>
        /// Send a message to the given topic.
        /// </summary>
        /// <param name="producer">The message producer</param>
        /// <param name="message">The message to send.</param>
        /// <param name="topicName">The name of the kafka topic to send the messages to.</param>
        /// <param name="partitionId">The partition to send messages to.</param>
        /// <param name="cancellationToken"></param>
        /// <returns>List of ProduceTopic response from each partition sent to or empty list if acks = 0.</returns>
        public static Task<ProduceResponse.Topic> SendMessageAsync(this IProducer producer, Message message, string topicName, int partitionId, CancellationToken cancellationToken)
        {
            return producer.SendMessageAsync(message, topicName, partitionId, null, cancellationToken);
        }

        /// <summary>
        /// Send a message to the given topic.
        /// </summary>
        /// <param name="producer">The message producer</param>
        /// <param name="message">The message to send.</param>
        /// <param name="topicName">The name of the kafka topic to send the messages to.</param>
        /// <param name="cancellationToken"></param>
        /// <returns>List of ProduceTopic response from each partition sent to or empty list if acks = 0.</returns>
        public static Task<IEnumerable<ProduceResponse.Topic>> SendMessageAsync(this IProducer producer, Message message, string topicName, CancellationToken cancellationToken)
        {
            return producer.SendMessageAsync(message, topicName, null, cancellationToken);
        }

        /// <summary>
        /// Send a message to the given topic.
        /// </summary>
        /// <param name="producer">The message producer</param>
        /// <param name="message">The message to send.</param>
        /// <param name="topicName">The name of the kafka topic to send the messages to.</param>
        /// <param name="partitionId">The partition to send messages to.</param>
        /// <param name="configuration">The configuration for sending the messages (ie acks, ack Timeout and codec)</param>
        /// <param name="cancellationToken">The token for cancellation</param>
        /// <returns>List of ProduceTopic response from each partition sent to or empty list if acks = 0.</returns>
        public static Task<ProduceResponse.Topic> SendMessageAsync(this IProducer producer, Message message, string topicName, int partitionId, ISendMessageConfiguration configuration, CancellationToken cancellationToken)
        {
            return producer.SendMessagesAsync(new[] { message }, topicName, partitionId, configuration, cancellationToken);
        }

        /// <summary>
        /// Send a message to the given topic.
        /// </summary>
        /// <param name="producer">The message producer</param>
        /// <param name="message">The message to send.</param>
        /// <param name="topicName">The name of the kafka topic to send the messages to.</param>
        /// <param name="configuration">The configuration for sending the messages (ie acks, ack Timeout and codec)</param>
        /// <param name="cancellationToken">The token for cancellation</param>
        /// <returns>List of ProduceTopic response from each partition sent to or empty list if acks = 0.</returns>
        public static Task<IEnumerable<ProduceResponse.Topic>> SendMessageAsync(this IProducer producer, Message message, string topicName, ISendMessageConfiguration configuration, CancellationToken cancellationToken)
        {
            return producer.SendMessagesAsync(new[] { message }, topicName, configuration, cancellationToken);
        }

        #endregion

        #region Consuming

        public static Task<int> FetchAsync(this IConsumer consumer, Func<Message, CancellationToken, Task> onMessageAsync, string topicName, int partitionId, long offset, CancellationToken cancellationToken, int? batchSize = null)
        {
            return consumer.FetchAsync(async (batch, token) => {
                foreach (var message in batch.Messages) {
                    await onMessageAsync(message, token).ConfigureAwait(false);
                }
            }, topicName, partitionId, offset, cancellationToken, batchSize);
        }

        public static async Task<int> FetchAsync(this IConsumer consumer, Func<IMessageBatch, CancellationToken, Task> onMessagesAsync, string topicName, int partitionId, long offset, CancellationToken cancellationToken, int? batchSize = null)
        {
            var total = 0;
            while (!cancellationToken.IsCancellationRequested) {
                var fetched = await consumer.FetchBatchAsync(topicName, partitionId, offset + total, cancellationToken, batchSize).ConfigureAwait(false);
                await onMessagesAsync(fetched, cancellationToken).ConfigureAwait(false);
                total += fetched.Messages.Count;
            }
            return total;
        }

        public static Task<IConsumerMember> JoinConsumerGroupAsync(this IConsumer consumer, string groupId, ConsumerProtocolMetadata metadata, CancellationToken cancellationToken)
        {
            return consumer.JoinGroupAsync(groupId, ConsumerEncoder.Protocol, new[] { metadata }, cancellationToken);
        }

        public static Task<IConsumerMember> JoinConsumerGroupAsync(this IConsumer consumer, string groupId, string protocolType, IMemberMetadata metadata, CancellationToken cancellationToken)
        {
            return consumer.JoinGroupAsync(groupId, protocolType, new[] { metadata }, cancellationToken);
        }

        public static async Task<IImmutableList<IMessageBatch>> FetchBatchesAsync(this IConsumerMember member, CancellationToken cancellationToken, int? batchSize = null)
        {
            var batches = new List<IMessageBatch>();
            IMessageBatch batch;
            while (!(batch = await member.FetchBatchAsync(cancellationToken, batchSize).ConfigureAwait(false)).IsEmpty()) {
                batches.Add(batch);
            }
            return batches.ToImmutableList();
        }

        public static async Task FetchUntilDisposedAsync(this IConsumerMember member, Func<IMessageBatch, CancellationToken, Task> onMessagesAsync, CancellationToken cancellationToken, int? batchSize = null)
        {
            try {
                await member.FetchAsync(onMessagesAsync, cancellationToken, batchSize);
            } catch (ObjectDisposedException) {
                // ignore
            }
        }

        public static async Task FetchAsync(this IConsumerMember member, Func<IMessageBatch, CancellationToken, Task> onMessagesAsync, CancellationToken cancellationToken, int? batchSize = null)
        {
            var tasks = new List<Task>();
            while (!cancellationToken.IsCancellationRequested) {
                var batches = await member.FetchBatchesAsync(cancellationToken, batchSize).ConfigureAwait(false);
                tasks.AddRange(batches.Select(async batch => await batch.FetchAsync(onMessagesAsync, member.Log, cancellationToken).ConfigureAwait(false)));
                if (tasks.Count == 0) break;
                await Task.WhenAny(tasks).ConfigureAwait(false);
                tasks = tasks.Where(t => !t.IsCompleted).ToList();
            }
        }

        public static async Task<long> CommitMarkedIgnoringDisposedAsync(this IMessageBatch batch, CancellationToken cancellationToken)
        {
            try {
                return await batch.CommitMarkedAsync(cancellationToken);
            } catch (ObjectDisposedException) {
                // ignore
                return 0;
            }
        }

        public static Task CommitAsync(this IMessageBatch batch, CancellationToken cancellationToken)
        {
            if (batch.Messages.Count == 0) return Task.FromResult(0);

            return batch.CommitAsync(batch.Messages[batch.Messages.Count - 1], cancellationToken);
        }

        public static Task CommitAsync(this IMessageBatch batch, Message message, CancellationToken cancellationToken)
        {
            batch.MarkSuccessful(message);
            return batch.CommitMarkedAsync(cancellationToken);
        }

        public static bool IsEmpty(this IMessageBatch batch)
        {
            return batch?.Messages?.Count == 0;
        }

        public static async Task FetchAsync(this IMessageBatch batch, Func<IMessageBatch, CancellationToken, Task> onMessagesAsync, ILog log, CancellationToken cancellationToken)
        {
            try {
                do {
                    using (var source = new CancellationTokenSource()) {
                        batch.OnDisposed = source.Cancel;
                        using (cancellationToken.Register(source.Cancel)) {
                            await onMessagesAsync(batch, source.Token).ConfigureAwait(false);
                        }
                        batch.OnDisposed = null;
                    }
                    batch = await batch.FetchNextAsync(cancellationToken).ConfigureAwait(false);
                } while (!batch.IsEmpty() && !cancellationToken.IsCancellationRequested);
            } catch (ObjectDisposedException ex) {
                log.Info(() => LogEvent.Create(ex));
            } catch (OperationCanceledException ex) {
                log.Verbose(() => LogEvent.Create(ex));
            } catch (Exception ex) {
                log.Error(LogEvent.Create(ex));
                throw;
            }
        }

        #endregion

        #region Router

        /// <exception cref="RoutingException">Thrown if the cached metadata for the given topic is invalid or missing.</exception>
        /// <exception cref="FetchOutOfRangeException">Thrown if the fetch request is not valid.</exception>
        /// <exception cref="TimeoutException">Thrown if there request times out</exception>
        /// <exception cref="ConnectionException">Thrown in case of network error contacting broker (after retries), or if none of the default brokers can be contacted.</exception>
        /// <exception cref="RequestException">Thrown in case of an unexpected error in the request</exception>
        public static async Task<T> SendAsync<T>(this IRouter router, IRequest<T> request, string topicName, int partitionId, CancellationToken cancellationToken, IRequestContext context = null, IRetry retryPolicy = null) where T : class, IResponse
        {
            bool? metadataInvalid = false;
            var routedRequest = new RoutedTopicRequest<T>(request, topicName, partitionId, router.Log);

            return await (retryPolicy ?? router.Configuration.SendRetry).TryAsync(
                async (retryAttempt, elapsed) => {
                    metadataInvalid = await router.RefreshTopicMetadataIfInvalidAsync(topicName, metadataInvalid, cancellationToken).ConfigureAwait(false);
                    await routedRequest.SendAsync(router, cancellationToken, context).ConfigureAwait(false);
                    return routedRequest.MetadataRetryResponse(retryAttempt, out metadataInvalid);
                },
                (ex, retryAttempt, retryDelay) => routedRequest.OnRetry(ex, out metadataInvalid),
                routedRequest.ThrowExtractedException,
                cancellationToken).ConfigureAwait(false);
        }

        /// <exception cref="RoutingException">Thrown if the cached metadata for the given topic is invalid or missing.</exception>
        /// <exception cref="FetchOutOfRangeException">Thrown if the fetch request is not valid.</exception>
        /// <exception cref="TimeoutException">Thrown if there request times out</exception>
        /// <exception cref="ConnectionException">Thrown in case of network error contacting broker (after retries), or if none of the default brokers can be contacted.</exception>
        /// <exception cref="RequestException">Thrown in case of an unexpected error in the request</exception>
        public static async Task<T> SendAsync<T>(this IRouter router, IRequest<T> request, string groupId, CancellationToken cancellationToken, IRequestContext context = null, IRetry retryPolicy = null) where T : class, IResponse
        {
            bool? metadataInvalid = false;
            var routedRequest = new RoutedGroupRequest<T>(request, groupId, router.Log);

            return await (retryPolicy ?? router.Configuration.SendRetry).TryAsync(
                async (retryAttempt, elapsed) => {
                    routedRequest.LogAttempt(retryAttempt);
                    metadataInvalid = await router.RefreshGroupMetadataIfInvalidAsync(groupId, metadataInvalid, cancellationToken).ConfigureAwait(false);
                    await routedRequest.SendAsync(router, cancellationToken, context).ConfigureAwait(false);
                    return routedRequest.MetadataRetryResponse(retryAttempt, out metadataInvalid);
                },
                (ex, retryAttempt, retryDelay) => routedRequest.OnRetry(ex, out metadataInvalid),
                routedRequest.ThrowExtractedException,
                cancellationToken).ConfigureAwait(false);
        }

        public static async Task<T> SendToAnyAsync<T>(this IRouter router, IRequest<T> request, CancellationToken cancellationToken, IRequestContext context = null) where T : class, IResponse
        {
            Exception lastException = null;
            var endpoints = new List<Endpoint>();
            foreach (var connection in router.Connections) {
                var endpoint = connection.Endpoint;
                try {
                    return await connection.SendAsync(request, cancellationToken, context).ConfigureAwait(false);
                } catch (Exception ex) {
                    lastException = ex;
                    endpoints.Add(endpoint);
                    router.Log.Info(() => LogEvent.Create(ex, $"Failed to contact {endpoint} -> Trying next server"));
                }
            }

            throw new ConnectionException(endpoints, lastException);
        }

        internal static async Task<bool> RefreshGroupMetadataIfInvalidAsync(this IRouter router, string groupId, bool? metadataInvalid, CancellationToken cancellationToken)
        {
            if (metadataInvalid.GetValueOrDefault(true)) {
                // unknown metadata status should not force the issue
                await router.RefreshGroupConnectionAsync(groupId, metadataInvalid.GetValueOrDefault(), cancellationToken).ConfigureAwait(false);
            }
            return false;
        }

        internal static async Task<bool> RefreshTopicMetadataIfInvalidAsync(this IRouter router, string topicName, bool? metadataInvalid, CancellationToken cancellationToken)
        {
            if (metadataInvalid.GetValueOrDefault(true)) {
                // unknown metadata status should not force the issue
                await router.RefreshTopicMetadataAsync(topicName, metadataInvalid.GetValueOrDefault(), cancellationToken).ConfigureAwait(false);
            }
            return false;
        }

        internal static void ThrowExtractedException<T>(this RoutedTopicRequest<T>[] routedTopicRequests) where T : class, IResponse
        {
            throw routedTopicRequests.Select(_ => _.ResponseException).FlattenAggregates();
        }

        internal static void MetadataRetry<T>(this IEnumerable<RoutedTopicRequest<T>> brokeredRequests, Exception exception, out bool? shouldRetry) where T : class, IResponse
        {
            shouldRetry = null;
            foreach (var brokeredRequest in brokeredRequests) {
                bool? requestRetry;
                brokeredRequest.OnRetry(exception, out requestRetry);
                if (requestRetry.HasValue) {
                    shouldRetry = requestRetry;
                }
            }
        }

        internal static bool IsPotentiallyRecoverableByMetadataRefresh(this Exception exception)
        {
            return exception is FetchOutOfRangeException
                || exception is TimeoutException
                || exception is ConnectionException
                || exception is RoutingException;
        }

        /// <summary>
        /// Given a collection of server connections, query for the topic metadata.
        /// </summary>
        /// <param name="router">The router which provides the route and metadata.</param>
        /// <param name="request">Metadata request to make</param>
        /// <param name="cancellationToken"></param>
        /// <remarks>
        /// Used by <see cref="Router"/> internally. Broken out for better testability, but not intended to be used separately.
        /// </remarks>
        /// <returns>MetadataResponse validated to be complete.</returns>
        internal static async Task<MetadataResponse> GetMetadataAsync(this IRouter router, MetadataRequest request, CancellationToken cancellationToken)
        {
            return await router.Configuration.RefreshRetry.TryAsync(
                async (retryAttempt, elapsed) => {
                    var connections = router.Connections.ToList();
                    var connection = connections[retryAttempt % connections.Count];
                    var response = await connection.SendAsync(request, cancellationToken).ConfigureAwait(false);
                    if (response == null) return new RetryAttempt<MetadataResponse>(null);

                    var results = response.brokers
                        .Select(ValidateBroker)
                        .Union(response.topic_metadata.Select(ValidateTopic))
                        .Where(r => !r.IsValid.GetValueOrDefault())
                        .ToList();

                    var exceptions = results.Select(r => r.ToException(connection.Endpoint)).Where(e => e != null).ToList();
                    if (exceptions.Count == 1) throw exceptions.Single();
                    if (exceptions.Count > 1) throw new AggregateException(exceptions);

                    if (results.Count == 0) return new RetryAttempt<MetadataResponse>(response);
                    foreach (var result in results.Where(r => !string.IsNullOrEmpty(r.Message))) {
                        router.Log.Warn(() => LogEvent.Create(result.Message));
                    }

                    return new RetryAttempt<MetadataResponse>(response, false);
                },
                (ex, retryAttempt, retryDelay) => router.Log.Warn(() => LogEvent.Create(ex, $"Failed metadata request on attempt {retryAttempt}: Will retry in {retryDelay}")),
                null, // return the failed response above, resulting in the final response
                cancellationToken).ConfigureAwait(false);
        }

        private class MetadataResult
        {
            public bool? IsValid { get; }
            public string Message { get; }
            private readonly ErrorCode _errorCode;

            public Exception ToException(Endpoint endpoint)
            {
                if (IsValid.GetValueOrDefault(true)) return null;

                if (_errorCode.IsSuccess()) return new ConnectionException(Message);
                return new RequestException(ApiKey.Metadata, _errorCode, endpoint, Message);
            }

            public MetadataResult(ErrorCode errorCode = ErrorCode.NONE, bool? isValid = null, string message = null)
            {
                Message = message ?? "";
                _errorCode = errorCode;
                IsValid = isValid;
            }
        }

        private static MetadataResult ValidateBroker(Server server)
        {
            if (server.Id == -1)                   return new MetadataResult(ErrorCode.UNKNOWN);
            if (string.IsNullOrEmpty(server.Host)) return new MetadataResult(ErrorCode.NONE, false, "Broker missing host information.");
            if (server.Port <= 0)                  return new MetadataResult(ErrorCode.NONE, false, "Broker missing port information.");
            return new MetadataResult(isValid: true);
        }

        private static MetadataResult ValidateTopic(MetadataResponse.Topic topic)
        {
            var errorCode = topic.topic_error_code;
            if (errorCode == ErrorCode.NONE) return new MetadataResult(isValid: true);
            if (errorCode.IsRetryable())     return new MetadataResult(errorCode, null, $"topic/{topic.topic} returned error code of {errorCode}: Retrying");
            return new MetadataResult(errorCode, false, $"topic/{topic.topic} returned an error of {errorCode}");
        }

        #endregion
    }
}