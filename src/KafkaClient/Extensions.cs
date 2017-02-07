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

        /// <summary>
        /// Get offsets for all partitions of a given topic.
        /// </summary>
        /// <param name="router">The router which provides the route and metadata.</param>
        /// <param name="topicName">Name of the topic to get offset information from.</param>
        /// <param name="maxOffsets">How many to get, at most.</param>
        /// <param name="offsetTime">These are best described by <see cref="OffsetsRequest.Topic.timestamp"/></param>
        /// <param name="cancellationToken"></param>
        public static Task<IImmutableList<OffsetsResponse.Topic>> GetOffsetsAsync(this IRouter router, string topicName, int maxOffsets, long offsetTime, CancellationToken cancellationToken)
        {
            return router.GetOffsetsAsync<OffsetsRequest, OffsetsResponse, OffsetsResponse.Topic>(
                topicName,
                partitions =>
                    new OffsetsRequest(
                        partitions.Select(
                            _ => new OffsetsRequest.Topic(topicName, _.partition_id, offsetTime, maxOffsets))),
                cancellationToken);
        }

        /// <summary>
        /// Get offsets for all partitions of a given topic.
        /// </summary>
        /// <param name="router">The router which provides the route and metadata.</param>
        /// <param name="topicName">Name of the topic to get offset information from.</param>
        /// <param name="cancellationToken"></param>
        public static Task<IImmutableList<OffsetsResponse.Topic>> GetOffsetsAsync(this IRouter router, string topicName, CancellationToken cancellationToken)
        {
            return router.GetOffsetsAsync(topicName, OffsetsRequest.Topic.DefaultMaxOffsets, OffsetsRequest.Topic.LatestTime, cancellationToken);
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
        public static async Task<OffsetsResponse.Topic> GetOffsetAsync(this IRouter router, string topicName, int partitionId, int maxOffsets, long offsetTime, CancellationToken cancellationToken)
        {
            var request = new OffsetsRequest(new OffsetsRequest.Topic(topicName, partitionId, offsetTime, maxOffsets));
            var response = await router.SendAsync(request, topicName, partitionId, cancellationToken).ConfigureAwait(false);
            return response.responses.SingleOrDefault(t => t.topic == topicName && t.partition_id == partitionId);
        }

        /// <summary>
        /// Get offsets for a single partitions of a given topic.
        /// </summary>
        public static Task<OffsetsResponse.Topic> GetOffsetAsync(this IRouter router, string topicName, int partitionId, CancellationToken cancellationToken)
        {
            return router.GetOffsetAsync(topicName, partitionId, OffsetsRequest.Topic.DefaultMaxOffsets, OffsetsRequest.Topic.LatestTime, cancellationToken);
        }

        /// <summary>
        /// Get group offsets for a single partition of a given topic.
        /// </summary>
        /// <param name="router">The router which provides the route and metadata.</param>
        /// <param name="topicName">Name of the topic to get offset information from.</param>
        /// <param name="partitionId">The partition to get offsets for.</param>
        /// <param name="groupId">The id of the consumer group</param>
        /// <param name="cancellationToken"></param>
        public static async Task<OffsetFetchResponse.Topic> GetOffsetAsync(this IRouter router, string topicName, int partitionId, string groupId, CancellationToken cancellationToken)
        {
            var request = new OffsetFetchRequest(groupId, new TopicPartition(topicName, partitionId));
            var response = await router.SendAsync(request, topicName, partitionId, cancellationToken).ConfigureAwait(false);
            return response.responses.SingleOrDefault(t => t.topic == topicName && t.partition_id == partitionId);
        }

        /// <summary>
        /// Get offsets for all partitions of a given topic.
        /// </summary>
        /// <param name="router">The router which provides the route and metadata.</param>
        /// <param name="topicName">Name of the topic to get offset information from.</param>
        /// <param name="groupId">The id of the consumer group</param>
        /// <param name="cancellationToken"></param>
        public static Task<IImmutableList<OffsetFetchResponse.Topic>> GetOffsetsAsync(this IRouter router, string topicName, string groupId, CancellationToken cancellationToken)
        {
            return router.GetOffsetsAsync<OffsetFetchRequest, OffsetFetchResponse, OffsetFetchResponse.Topic>(
                topicName,
                partitions =>
                    new OffsetFetchRequest(
                        groupId, partitions.Select(_ => new OffsetsRequest.Topic(topicName, _.partition_id))),
                cancellationToken);
        }

        /// <summary>
        /// Get offsets for all partitions of a given topic.
        /// </summary>
        private static async Task<IImmutableList<TTopicResponse>> GetOffsetsAsync<TRequest, TResponse, TTopicResponse>(
            this IRouter router, 
            string topicName, 
            Func<IGrouping<int, MetadataResponse.Partition>, TRequest> requestFunc, 
            CancellationToken cancellationToken
            )
            where TRequest : class, IRequest<TResponse>
            where TResponse : class, IResponse<TTopicResponse>
            where TTopicResponse : TopicResponse
        {
            bool? metadataInvalid = false;
            var offsets = new Dictionary<int, TTopicResponse>();
            RoutedTopicRequest<TResponse>[] routedTopicRequests = null;

            return await router.Configuration.SendRetry.TryAsync(
                async (retryAttempt, elapsed) => {
                    metadataInvalid = await router.RefreshTopicMetadataIfInvalidAsync(topicName, metadataInvalid, cancellationToken).ConfigureAwait(false);

                    var topicMetadata = await router.GetTopicMetadataAsync(topicName, cancellationToken).ConfigureAwait(false);
                    routedTopicRequests = topicMetadata
                        .partition_metadata
                        .Where(_ => !offsets.ContainsKey(_.partition_id)) // skip partitions already successfully retrieved
                        .GroupBy(x => x.leader)
                        .Select(partitions => 
                            new RoutedTopicRequest<TResponse>(requestFunc(partitions),
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
                        ? new RetryAttempt<IImmutableList<TTopicResponse>>(offsets.Values.ToImmutableList()) 
                        : RetryAttempt<IImmutableList<TTopicResponse>>.Retry;
                },
                (ex, retryAttempt, retryDelay) => routedTopicRequests.MetadataRetry(ex, out metadataInvalid),
                routedTopicRequests.ThrowExtractedException, 
                cancellationToken).ConfigureAwait(false);
        }

        #endregion
    }
}