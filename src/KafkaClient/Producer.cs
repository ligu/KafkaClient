using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Connection;
using KafkaClient.Protocol;
using Nito.AsyncEx;

namespace KafkaClient
{
    /// <summary>
    /// Provides a simplified high level API for producing messages on a topic.
    /// </summary>
    public class Producer : IProducer
    {
        private readonly CancellationTokenSource _stopToken = new CancellationTokenSource();
        private readonly ConcurrentQueue<ProduceTopicTask> _internalQueue;
        private readonly AsyncCollection<ProduceTopicTask> _produceMessageQueue;
        private readonly SemaphoreSlim _produceRequestSemaphore;
        private readonly Task _batchSendTask;

        private int _inFlightMessageCount;

        /// <summary>
        /// Get the number of messages sitting in the buffer waiting to be sent.
        /// </summary>
        public int BufferedMessageCount => _internalQueue.Count;

        /// <summary>
        /// Get the number of messages staged for Async upload.
        /// </summary>
        public int InFlightMessageCount => _inFlightMessageCount;

        /// <summary>
        /// Get the number of active async threads sending messages.
        /// </summary>
        public int ActiveSenders => Configuration.RequestParallelization - _produceRequestSemaphore.CurrentCount;

        /// <inheritdoc />
        public IBrokerRouter BrokerRouter { get; }

        public IProducerConfiguration Configuration { get; }

        /// <summary>
        /// Construct a Producer class.
        /// </summary>
        /// <param name="brokerRouter">The router used to direct produced messages to the correct partition.</param>
        /// <param name="configuration">The configuration parameters.</param>
        /// <remarks>
        /// The <see cref="IProducerConfiguration.RequestParallelization"/> parameter provides a mechanism for minimizing the amount of 
        /// async requests in flight at any one time by blocking the caller requesting the async call. This effectively puts an upper 
        /// limit on the amount of times a caller can call SendMessagesAsync before the caller is blocked.
        ///
        /// The <see cref="IProducerConfiguration.BatchSize"/> parameter provides a way to limit the max amount of memory the driver uses 
        /// should the send pipeline get overwhelmed and the buffer starts to fill up.  This is an inaccurate limiting memory use as the 
        /// amount of memory actually used is dependant on the general message size being buffered.
        ///
        /// A message will start its timeout countdown as soon as it is added to the producer async queue. If there are a large number of
        /// messages sitting in the async queue then a message may spend its entire timeout cycle waiting in this queue and never getting
        /// attempted to send to Kafka before a timeout exception is thrown.
        /// </remarks>
        public Producer(IBrokerRouter brokerRouter, IProducerConfiguration configuration = null)
        {
            BrokerRouter = brokerRouter;
            Configuration = configuration ?? new ProducerConfiguration();
            _internalQueue = new ConcurrentQueue<ProduceTopicTask>();
            _produceMessageQueue = new AsyncCollection<ProduceTopicTask>(_internalQueue);
            _produceRequestSemaphore = new SemaphoreSlim(Configuration.RequestParallelization, Configuration.RequestParallelization);
            _batchSendTask = Task.Run(BatchSendAsync, _stopToken.Token);
        }

        /// <inheritdoc />
        public async Task<ProduceTopic[]> SendMessagesAsync(IEnumerable<Message> messages, string topicName, int? partition, ISendMessageConfiguration configuration, CancellationToken cancellationToken)
        {
            var produceTopicTasks = messages.Select(message => new ProduceTopicTask(topicName, partition, message, configuration ?? Configuration.SendDefaults, cancellationToken)).ToArray();
            _produceMessageQueue.AddRange(produceTopicTasks, cancellationToken); 
            return await Task.WhenAll(produceTopicTasks.Select(x => x.Tcs.Task)).ConfigureAwait(false);
        }

        /// <summary>
        /// Stops the producer from accepting new messages, waiting for in-flight messages to be sent before returning.
        /// </summary>
        public async Task StopAsync(CancellationToken cancellationToken)
        {
            // block incoming data
            _produceMessageQueue.CompleteAdding();
            await Task.WhenAny(_batchSendTask, Task.Delay(Configuration.StopTimeout, cancellationToken)).ConfigureAwait(false);
            _stopToken.Cancel();
        }

        private async Task BatchSendAsync()
        {
            BrokerRouter.Log.InfoFormat("Producer sending task starting");
            try {
                while (true) {
                    var batch = ImmutableList<ProduceTopicTask>.Empty;

                    try {
                        try {
                            if (await _produceMessageQueue.OutputAvailableAsync(_stopToken.Token).ConfigureAwait(false)) {
                                using (var cancellation = new TimedCancellation(_stopToken.Token, Configuration.BatchMaxDelay)) {
                                    while (batch.Count < Configuration.BatchSize && !cancellation.Token.IsCancellationRequested) {
                                        var produceTopicTask = await _produceMessageQueue.TakeAsync(cancellation.Token).ConfigureAwait(false);
                                        if (produceTopicTask.CancellationToken.IsCancellationRequested) {
                                            produceTopicTask.Tcs.SetCanceled();
                                        } else {
                                            batch = batch.Add(produceTopicTask);
                                        }
                                    }
                                }
                            }
                        } catch (OperationCanceledException ex) {
                            BrokerRouter.Log.DebugFormat(ex);
                        }

                        // in case the collection has been closed ...
                        batch = batch.AddRange(_produceMessageQueue.GetConsumingEnumerable(new CancellationToken(true)));

                        if (!batch.IsEmpty) {
                            foreach (var codec in new[] {MessageCodec.CodecNone, MessageCodec.CodecGzip}) {
                                await ProduceAndSendBatchAsync(batch, codec).ConfigureAwait(false);
                            }
                        }
                    } catch (Exception ex) {
                        batch.ForEach(x => x.Tcs.TrySetException(ex));
                    }
                }
            } catch(Exception ex) { 
                BrokerRouter.Log.WarnFormat(ex, "Error during producer send");
            } finally {
                BrokerRouter.Log.InfoFormat("Producer sending task ending");
            }
        }

        private async Task ProduceAndSendBatchAsync(IEnumerable<ProduceTopicTask> batch, MessageCodec codec)
        {
            var filteredBatch = batch.Where(ptt => ptt.Codec == codec).ToImmutableList();
            if (filteredBatch.IsEmpty) return;

            var batchToken = filteredBatch[0].CancellationToken;
            if (filteredBatch.TrueForAll(p => p.CancellationToken == batchToken)) {
                using (var merged = new MergedCancellation(_stopToken.Token, batchToken)) {
                    await ProduceAndSendBatchAsync(filteredBatch, codec, merged.Token).ConfigureAwait(false);
                }
            } else {
                await ProduceAndSendBatchAsync(filteredBatch, codec, _stopToken.Token).ConfigureAwait(false);
            }
        }
        private async Task ProduceAndSendBatchAsync(IReadOnlyCollection<ProduceTopicTask> produceTasks, MessageCodec codec, CancellationToken cancellationToken)
        {
            Interlocked.Add(ref _inFlightMessageCount, produceTasks.Count);
            try {
                await BrokerRouter.GetTopicMetadataAsync(produceTasks.Select(m => m.TopicName), cancellationToken).ConfigureAwait(false);

                // we must send a different produce request for each ack level and timeout combination.
                // we must also send requests to the correct broker / endpoint
                var endpointGroups = produceTasks.Select(
                    ptt => new {
                        ProduceTask = ptt,
                        Route = ptt.Partition.HasValue
                            ? BrokerRouter.GetBrokerRoute(ptt.TopicName, ptt.Partition.Value)
                            : BrokerRouter.GetBrokerRoute(ptt.TopicName, ptt.Message.Key)
                    })
                    .GroupBy(_ => new {_.ProduceTask.Acks, _.ProduceTask.AckTimeout, _.Route.Connection.Endpoint});

                var sendBatches = new List<ProduceTaskBatch>();
                foreach (var endpointGroup in endpointGroups) {
                    var produceTasksByTopicPayload = endpointGroup
                        .GroupBy(_ => new Topic(_.Route.TopicName, _.Route.PartitionId))
                        .ToImmutableDictionary(g => g.Key, g => g.Select(_ => _.ProduceTask).ToImmutableList());
                    var payloads = produceTasksByTopicPayload.Select(p => new Payload(p.Key.TopicName, p.Key.PartitionId, p.Value.Select(_ => _.Message), codec));
                    var request = new ProduceRequest(payloads, endpointGroup.Key.AckTimeout, endpointGroup.Key.Acks);

                    var connection = endpointGroup.Select(_ => _.Route).First().Connection; // they'll all be the same since they're grouped by this
                    await _produceRequestSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
                    var sendGroupTask = connection.SendAsync(request, cancellationToken);
                    // ReSharper disable once UnusedVariable
                    var continuation = sendGroupTask.ContinueWith(t => _produceRequestSemaphore.Release(), CancellationToken.None);
                    sendBatches.Add(new ProduceTaskBatch(connection.Endpoint, endpointGroup.Key.Acks, sendGroupTask, produceTasksByTopicPayload));
                }

                try {
                    await Task.WhenAll(sendBatches.Select(batch => batch.ReceiveTask)).ConfigureAwait(false);
                } catch (Exception ex) {
                    BrokerRouter.Log.ErrorFormat(ex);
                }
                await SetResult(sendBatches).ConfigureAwait(false);
            } finally {
                Interlocked.Add(ref _inFlightMessageCount, -produceTasks.Count);
            }
        }

        private async Task SetResult(IEnumerable<ProduceTaskBatch> batches)
        {
            foreach (var batch in batches) {
                try {
                    var batchResult = await batch.ReceiveTask.ConfigureAwait(false); // await triggers exceptions correctly

                    foreach (var topic in batch.TasksByTopicPayload.Keys.Except(batchResult.Topics)) {
                        BrokerRouter.Log.WarnFormat("produce batch on {0} included topic{1}/partition{2} but had no corresponding response", batch.Endpoint, topic.TopicName, topic.PartitionId);
                        foreach (var task in batch.TasksByTopicPayload[topic]) {
                            task.Tcs.SetResult(null);
                        }
                    }

                    foreach (var topic in batchResult.Topics) {
                        ImmutableList<ProduceTopicTask> tasks;
                        if (!batch.TasksByTopicPayload.TryGetValue(topic, out tasks)) {
                            BrokerRouter.Log.ErrorFormat("produce batch on {0} got response for topic{1}/partition{2} but had no corresponding request", batch.Endpoint, topic.TopicName, topic.PartitionId);
                            continue;
                        }

                        var offsetCount = 0;
                        foreach (var task in tasks) {
                            task.Tcs.SetResult(batch.Acks == 0
                                ? new ProduceTopic(topic.TopicName, topic.PartitionId, ErrorResponseCode.NoError, -1)
                                : new ProduceTopic(topic.TopicName, topic.PartitionId, topic.ErrorCode, topic.Offset + offsetCount, topic.Timestamp));
                            offsetCount += 1;
                        }
                    }
                } catch (Exception ex) {
                    BrokerRouter.Log.ErrorFormat(ex, "failed to send batch to {0} with acks {1}", batch.Endpoint, batch.Acks);
                    foreach (var productTopicTask in batch.TasksByTopicPayload.Values.SelectMany(_ => _)) {
                        productTopicTask.Tcs.TrySetException(ex);
                    }
                }
            }
        }

        public void Dispose()
        {
            // block incoming data
            _produceMessageQueue.CompleteAdding();
            _stopToken.Cancel();

            // cleanup
            using (_stopToken) {
                using (BrokerRouter)
                {
                }
            }
        }

        private class ProduceTopicTask : CancellableTask<ProduceTopic>
        {
            public ProduceTopicTask(string topicName, int? partition, Message message, ISendMessageConfiguration configuration, CancellationToken cancellationToken)
                : base(cancellationToken)
            {
                TopicName = topicName;
                Partition = partition;
                Message = message;
                Codec = configuration.Codec;
                Acks = configuration.Acks;
                AckTimeout = configuration.AckTimeout;
            }

            // where
            public string TopicName { get; }
            public int? Partition { get; }

            // what
            public Message Message { get; }
            public MessageCodec Codec { get; }

            // confirmation
            public short Acks { get; }
            public TimeSpan AckTimeout { get; }
        }

        private class ProduceTaskBatch
        {
            public ProduceTaskBatch(Endpoint endpoint, short acks, Task<ProduceResponse> receiveTask, ImmutableDictionary<Topic, ImmutableList<ProduceTopicTask>> tasksByTopicPayload)
            {
                Endpoint = endpoint;
                Acks = acks;
                ReceiveTask = receiveTask;
                TasksByTopicPayload = tasksByTopicPayload;
            }

            public Endpoint Endpoint { get; }
            public short Acks { get; }
            public Task<ProduceResponse> ReceiveTask { get; }
            public ImmutableDictionary<Topic, ImmutableList<ProduceTopicTask>> TasksByTopicPayload { get; }
        }
    }
}