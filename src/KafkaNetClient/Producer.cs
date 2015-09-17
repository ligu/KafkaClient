using KafkaNet.Common;
using KafkaNet.Protocol;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaNet
{
    /// <summary>
    /// Provides a simplified high level API for producing messages on a topic.
    /// </summary>
    public class Producer : IMetadataQueries
    {
        private const int MaxDisposeWaitSeconds = 30;
        private const int DefaultAckTimeoutMS = 1000;
        private const int MaximumAsyncRequests = 20;
        private const int MaximumMessageBuffer = 1000;
        private const int DefaultBatchDelayMS = 100;
        private const int DefaultBatchSize = 100;

        private readonly ProtocolGateway _protocolGateway;
        private readonly CancellationTokenSource _stopToken = new CancellationTokenSource();
        private readonly int _maximumAsyncRequests;
        private readonly AsyncCollection<TopicMessage> _asyncCollection;
        private readonly SemaphoreSlim _semaphoreMaximumAsync;
        private readonly IMetadataQueries _metadataQueries;
        private readonly Task _postTask;

        private int _inFlightMessageCount = 0;

        /// <summary>
        /// Get the number of messages sitting in the buffer waiting to be sent.
        /// </summary>
        public int BufferCount { get { return _asyncCollection.Count; } }

        /// <summary>
        /// Get the number of messages staged for Async upload.
        /// </summary>
        public int InFlightMessageCount { get { return _inFlightMessageCount; } }

        /// <summary>
        /// Get the number of active async threads sending messages.
        /// </summary>
        public int AsyncCount { get { return _maximumAsyncRequests - _semaphoreMaximumAsync.CurrentCount; } }

        /// <summary>
        /// The number of messages to wait for before sending to kafka. Will wait <see
        /// cref="BatchDelayTime"/> before sending whats received.
        /// </summary>
        public int BatchSize { get; set; }

        /// <summary>
        /// The time to wait for a batch size of <see cref="BatchSize"/> before sending messages to kafka.
        /// </summary>
        public TimeSpan BatchDelayTime { get; set; }

        /// <summary>
        /// The broker router this producer uses to route messages.
        /// </summary>
        public IBrokerRouter BrokerRouter { get; private set; }

        public Producer(IBrokerRouter brokerRouter, int maximumAsyncRequests = MaximumAsyncRequests, int maximumMessageBuffer = MaximumMessageBuffer)
        {
            BrokerRouter = brokerRouter;
            _protocolGateway = new ProtocolGateway(BrokerRouter);
            _maximumAsyncRequests = maximumAsyncRequests;
            _metadataQueries = new MetadataQueries(BrokerRouter);
            _asyncCollection = new AsyncCollection<TopicMessage>();
            _semaphoreMaximumAsync = new SemaphoreSlim(maximumAsyncRequests, maximumAsyncRequests);

            BatchSize = DefaultBatchSize;
            BatchDelayTime = TimeSpan.FromMilliseconds(DefaultBatchDelayMS);

            _postTask = Task.Run(() =>
            {
                BatchSendAsync();
                //TODO add log for ending the sending thread.
            });
        }

        /// <summary>
        /// Send an enumerable of message objects to a given topic.
        /// </summary>
        /// <param name="topic">The name of the kafka topic to send the messages to.</param>
        /// <param name="messages">The enumerable of messages that will be sent to the given topic.</param>
        /// <param name="acks">
        /// The required level of acknowlegment from the kafka server. 0=none, 1=writen to leader,
        /// 2+=writen to replicas, -1=writen to all replicas.
        /// </param>
        /// <param name="timeout">
        /// Interal kafka timeout to wait for the requested level of ack to occur before returning.
        /// Defaults to 1000ms.
        /// </param>
        /// <param name="codec">The codec to apply to the message collection. Defaults to none.</param>
        /// <returns>
        /// List of ProduceResponses from each partition sent to or empty list if acks = 0.
        /// </returns>
        public Task<ProduceResponse[]> SendMessageAsync(string topic, IEnumerable<Message> messages, Int16 acks = 1,
            TimeSpan? timeout = null, MessageCodec codec = MessageCodec.CodecNone, int? partition = null)
        {
            if (_stopToken.IsCancellationRequested)
                throw new ObjectDisposedException("Cannot send new documents as producer is disposing.");
            if (timeout == null) timeout = TimeSpan.FromMilliseconds(DefaultAckTimeoutMS);

            var batch = messages.Select(message => new TopicMessage
            {
                Acks = acks,
                Codec = codec,
                Timeout = timeout.Value,
                Topic = topic,
                Message = message,
                Partition = partition
            }).ToArray();

            _asyncCollection.AddRange(batch);

            return Task.WhenAll(batch.Select(x => x.Tcs.Task));
        }

        public Task<ProduceResponse[]> SendMessageAsync(string topic, int partition, params Message[] messages)
        {
            return SendMessageAsync(topic, messages, partition: partition);
        }

        public async Task<ProduceResponse> SendMessageAsync(Message messages, string topic, int partition, Int16 acks = 1)
        {
            var result = await SendMessageAsync(topic, new Message[] { messages }, partition: partition, acks: acks);
            return result.FirstOrDefault();
        }

        public Task<ProduceResponse[]> SendMessageAsync(string topic, params Message[] messages)
        {
            return SendMessageAsync(topic, messages, acks: 1);
        }

        /// <summary>
        /// Get the metadata about a given topic.
        /// </summary>
        /// <param name="topic">The name of the topic to get metadata for.</param>
        /// <returns>Topic with metadata information.</returns>
        public Topic GetTopicFromCache(string topic)
        {
            return _metadataQueries.GetTopicFromCache(topic);
        }

        public Task<List<OffsetResponse>> GetTopicOffsetAsync(string topic, int maxOffsets = 2, int time = -1)
        {
            return _metadataQueries.GetTopicOffsetAsync(topic, maxOffsets, time);
        }

        /// <summary>
        /// Stops the producer from accepting new messages, and optionally waits for in-flight
        /// messages to be sent before returning.
        /// </summary>
        /// <param name="waitForRequestsToComplete">
        /// True to wait for in-flight requests to complete, false otherwise.
        /// </param>
        /// <param name="maxWait">
        /// Maximum time to wait for in-flight requests to complete. Has no effect if
        /// <c>waitForRequestsToComplete</c> is false
        /// </param>
        public void Stop(bool waitForRequestsToComplete = true, TimeSpan? maxWait = null)
        {
            //block incoming data
            _asyncCollection.CompleteAdding();

            if (waitForRequestsToComplete)
            {
                //wait for the collection to drain
                _postTask.Wait(maxWait ?? TimeSpan.FromSeconds(MaxDisposeWaitSeconds));
            }

            _stopToken.Cancel();
        }

        private async Task BatchSendAsync()
        {
            while (IsNotDisposedOrHasMessagesToProcess())
            {
                List<TopicMessage> batch = null;

                try
                {
                    try
                    {
                        await _asyncCollection.OnHasDataAvailable(_stopToken.Token).ConfigureAwait(false);

                        batch = await _asyncCollection.TakeAsync(BatchSize, BatchDelayTime, _stopToken.Token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException ex)
                    {
                        //TODO log that the operation was canceled, this only happens during a dispose
                    }

                    if (_asyncCollection.IsCompleted && _asyncCollection.Count > 0)
                    {
                        batch = batch ?? new List<TopicMessage>(_asyncCollection.Count);

                        //Drain any messages remaining in the queue and add them to the send batch
                        batch.AddRange(_asyncCollection.Drain());
                    }
                    if (batch != null)
                        await ProduceAndSendBatchAsync(batch, _stopToken.Token).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    if (batch != null)
                    {
                        batch.ForEach(x => x.Tcs.TrySetException(ex));
                    }
                }
            }
        }

        private bool IsNotDisposedOrHasMessagesToProcess()
        {
            return _asyncCollection.IsCompleted == false || _asyncCollection.Count > 0;
        }

        private async Task ProduceAndSendBatchAsync(List<TopicMessage> messages, CancellationToken cancellationToken)
        {
            Interlocked.Add(ref _inFlightMessageCount, messages.Count);

            var topics = messages.GroupBy(batch => batch.Topic).Select(batch => batch.Key).ToArray();
            await BrokerRouter.RefreshMissingTopicMetadata(topics).ConfigureAwait(false);

            //we must send a different produce request for each ack level and timeout combination.
            foreach (var ackLevelBatch in messages.GroupBy(batch => new { batch.Acks, batch.Timeout }))
            {
                var messageByRouter = ackLevelBatch.Select(batch => new
                {
                    TopicMessage = batch,
                    AckLevel = ackLevelBatch.Key.Acks,
                    Route = batch.Partition.HasValue ? BrokerRouter.SelectBrokerRouteFromLocalCache(batch.Topic, batch.Partition.Value) : BrokerRouter.SelectBrokerRouteFromLocalCache(batch.Topic, batch.Message.Key)
                }).GroupBy(x => new { x.Route, x.TopicMessage.Topic, x.TopicMessage.Codec, x.AckLevel });

                var sendTasks = new List<BrokerRouteSendBatch>();
                foreach (var group in messageByRouter)
                {
                    var payload = new Payload
                    {
                        Codec = group.Key.Codec,
                        Topic = group.Key.Topic,
                        Partition = group.Key.Route.PartitionId,
                        Messages = group.Select(x => x.TopicMessage.Message).ToList()
                    };

                    var request = new ProduceRequest
                    {
                        Acks = ackLevelBatch.Key.Acks,
                        TimeoutMS = (int)ackLevelBatch.Key.Timeout.TotalMilliseconds,
                        Payload = new List<Payload> { payload }
                    };

                    await _semaphoreMaximumAsync.WaitAsync(cancellationToken).ConfigureAwait(false);

                    var sendGroupTask = _protocolGateway.SendProtocolRequest(request, group.Key.Topic, group.Key.Route.PartitionId);
                    var brokerSendTask = new BrokerRouteSendBatch
                    {
                        Route = group.Key.Route,
                        Task = sendGroupTask,
                        MessagesSent = group.Select(x => x.TopicMessage).ToList(),
                        AckLevel = group.Key.AckLevel
                    };

                    //ensure the async is released as soon as each task is completed //TODO: remove it from ack level 0 , don't like
                    brokerSendTask.Task.ContinueWith(t => { _semaphoreMaximumAsync.Release(); }, cancellationToken);

                    sendTasks.Add(brokerSendTask);
                }

                try
                {
                    await Task.WhenAll(sendTasks.Select(x => x.Task)).ConfigureAwait(false);
                }
                catch (Exception)
                {
                    //TODO:Add more log
                }

                await SetResult(sendTasks);
                Interlocked.Add(ref _inFlightMessageCount, messages.Count * -1);
            }
        }

        private static async Task SetResult(List<BrokerRouteSendBatch> sendTasks)
        {
            foreach (var sendTask in sendTasks)
            {
                try
                {
                    //all ready done don't need to await but it none blocking syntext
                    var batchResult = await sendTask.Task;
                    var numberOfMessage = sendTask.MessagesSent.Count;
                    for (int i = 0; i < numberOfMessage; i++)
                    {
                        bool isAckLevel0 = sendTask.AckLevel == 0;
                        if (isAckLevel0)
                        {
                            var responce = new ProduceResponse()
                            {
                                Error = (short)ErrorResponseCode.NoError,
                                PartitionId = sendTask.Route.PartitionId,
                                Topic = sendTask.Route.Topic,
                                Offset = -1
                            };
                            sendTask.MessagesSent[i].Tcs.SetResult(responce);
                        }
                        else
                        {
                            var responce = new ProduceResponse()
                            {
                                Error = batchResult.Error,
                                PartitionId = batchResult.PartitionId,
                                Topic = batchResult.Topic,
                                Offset = batchResult.Offset + i
                            };
                            sendTask.MessagesSent[i].Tcs.SetResult(responce);
                        }
                    }
                }
                catch (Exception ex)
                {
                    //TODO:Add more log
                    sendTask.MessagesSent.ForEach((x) => x.Tcs.TrySetException(ex));
                }
            }
        }

        #region Dispose...

        public void Dispose()
        {
            //Clients really should call Stop() first, but just in case they didn't...
            this.Stop(false);

            //dispose
            using (_stopToken)
            using (_metadataQueries)
            {
            }
        }

        #endregion Dispose...
    }

    internal class TopicMessage
    {
        public TaskCompletionSource<ProduceResponse> Tcs { get; set; }
        public short Acks { get; set; }
        public TimeSpan Timeout { get; set; }
        public MessageCodec Codec { get; set; }
        public string Topic { get; set; }
        public Message Message { get; set; }
        public int? Partition { get; set; }

        public TopicMessage()
        {
            Tcs = new TaskCompletionSource<ProduceResponse>();
        }
    }

    internal class BrokerRouteSendBatch
    {
        public short AckLevel { get; set; }
        public BrokerRoute Route { get; set; }
        public Task<ProduceResponse> Task { get; set; }
        public List<TopicMessage> MessagesSent { get; set; }
    }
}