using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Protocol;

namespace KafkaClient.Connection
{
    /// <summary>
    /// Connection represents the lowest level TCP stream connection to a Kafka broker.
    /// The Send and Receive are separated into two disconnected paths and must be combined outside
    /// this class by the correlation ID contained within the returned message.
    ///
    /// The SendAsync function will return a Task and complete once the data has been sent to the outbound stream.
    /// The Read response is handled by a single thread polling the stream for data and firing an OnResponseReceived
    /// event when a response is received.
    /// </summary>
    public class Connection : IConnection
    {
        public bool IsInErrorState { get; private set; }

        private readonly ConcurrentDictionary<int, AsyncRequestItem> _requestsByCorrelation = new ConcurrentDictionary<int, AsyncRequestItem>();
        private readonly ILog _log;
        private readonly ITcpSocket _socket;
        private readonly IConnectionConfiguration _configuration;

        private readonly CancellationTokenSource _disposeToken = new CancellationTokenSource();
        private int _disposeCount;

        private Task _readerTask;
        private int _activeReaderCount;
        private int _correlationIdSeed;

        /// <summary>
        /// Initializes a new instance of the Connection class.
        /// </summary>
        /// <param name="socket">The kafka socket initialized to the kafka server.</param>
        /// <param name="configuration">The configuration, including connection and request timeouts.</param>
        /// <param name="log">Logging interface used to record any log messages created by the connection.</param>
        public Connection(ITcpSocket socket, IConnectionConfiguration configuration = null, ILog log = null)
        {
            _socket = socket;
            _log = log ?? TraceLog.Log;
            _configuration = configuration ?? new ConnectionConfiguration();

            StartReader();
        }

        /// <summary>
        /// Indicates a thread is polling the stream for data to read.
        /// </summary>
        public bool IsReaderAlive => _activeReaderCount >= 1;

        /// <inheritdoc />
        public Endpoint Endpoint => _socket.Endpoint;

        /// <inheritdoc />
        public Task SendAsync(DataPayload payload, CancellationToken token)
        {
            return _socket.WriteAsync(payload, token);
        }

        /// <summary>
        /// Send kafka payload to server and receive a task event when response is received.
        /// </summary>
        /// <typeparam name="T">A Kafka response object return by decode function.</typeparam>
        /// <param name="request">The IRequest to send to the kafka servers.</param>
        /// <param name="context">The context for the request.</param>
        /// <param name="token">Cancellation token used to cancel the transfer.</param>
        /// <returns></returns>
        public async Task<T> SendAsync<T>(IRequest<T> request, CancellationToken token, IRequestContext context = null) where T : class, IResponse
        {
            context = context.WithCorrelation(NextCorrelationId());

            _log.Debug(() => LogEvent.Create($"SendAsync Api {request.ApiKey} CorrelationId {context.CorrelationId} to {Endpoint}"));
            if (!request.ExpectResponse) {
                await _socket.WriteAsync(KafkaEncoder.Encode(context, request), token).ConfigureAwait(false);
                return default(T);
            }

            using (var asyncRequest = new AsyncRequestItem(context.CorrelationId, _configuration.RequestTimeout)) {
                try {
                    AddAsyncRequestItemToResponseQueue(asyncRequest);
                    ExceptionDispatchInfo exceptionDispatchInfo = null;

                    try {
                        await _socket.WriteAsync(KafkaEncoder.Encode(context, request), token).ConfigureAwait(false);
                    } catch (Exception ex) {
                        exceptionDispatchInfo = ExceptionDispatchInfo.Capture(ex);
                    }

                    asyncRequest.MarkRequestAsSent(exceptionDispatchInfo, TriggerMessageTimeout);
                } catch (OperationCanceledException) {
                    TriggerMessageTimeout(asyncRequest);
                }

                var response = await asyncRequest.ReceiveTask.Task.ThrowIfCancellationRequested(token).ConfigureAwait(false);
                return KafkaEncoder.Decode<T>(context, response);
            }
        }

        #region Equals

        public override bool Equals(object obj)
        {
            return Equals(obj as Connection);
        }

        protected bool Equals(Connection other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Equals(_socket.Endpoint, other.Endpoint);
        }

        public override int GetHashCode()
        {
            return _socket.Endpoint != null ? _socket.Endpoint.GetHashCode() : 0;
        }

        #endregion Equals

        private void StartReader()
        {
            // This thread will poll the receive stream for data, parse a message out
            // and trigger an event with the message payload
            _readerTask = Task.Run(async () => {
                try {
                    // only allow one reader to execute, dump out all other requests
                    if (Interlocked.Increment(ref _activeReaderCount) != 1) return;

                    while (_disposeToken.IsCancellationRequested == false) {
                        try {
                            _log.Debug(() => LogEvent.Create($"Awaiting message from {_socket.Endpoint}"));
                            var messageSizeResult = await _socket.ReadAsync(4, _disposeToken.Token).ConfigureAwait(false);
                            var messageSize = messageSizeResult.ToInt32();

                            _log.Debug(() => LogEvent.Create($"Received message of size {messageSize} from {_socket.Endpoint}"));
                            var message = await _socket.ReadAsync(messageSize, _disposeToken.Token).ConfigureAwait(false);

                            CorrelatePayloadToRequest(message);
                            if (IsInErrorState) {
                                _log.Info(() => LogEvent.Create($"Polling read thread has recovered on {_socket.Endpoint}"));
                            }

                            IsInErrorState = false;
                        } catch (Exception ex) {
                            // this may have unexpected behavior for exceptions that don't result in an error state or cancellation since the 
                            // resulting position in the tcp stream is likely wrong ...

                            // Also, it is possible for orphaned requests to exist in the _requestsByCorrelation after failure

                            //don't record the exception if we are disposing
                            if (_disposeToken.IsCancellationRequested == false) {
                                //TODO being in sync with the byte order on read is important.  What happens if this exception causes us to be out of sync?
                                //record exception and continue to scan for data.

                                //TODO create an event on kafkaTcpSocket and resume only when the connection is online
                                if (!IsInErrorState) {
                                    _log.Error(LogEvent.Create(ex, $"Polling read thread {_socket.Endpoint}"));
                                    IsInErrorState = true;
                                }
                            }
                        }
                    }
                } finally {
                    Interlocked.Decrement(ref _activeReaderCount);
                    _log.Debug(() => LogEvent.Create($"Closed down connection to {_socket.Endpoint}"));
                }
            });
        }

        private void CorrelatePayloadToRequest(byte[] payload)
        {
            var correlationId = payload.Take(4).ToArray().ToInt32();
            AsyncRequestItem asyncRequest;
            if (_requestsByCorrelation.TryRemove(correlationId, out asyncRequest)) {
                _log.Debug(() => LogEvent.Create($"Matched Response from {Endpoint} with CorrelationId {correlationId}"));
                asyncRequest.ReceiveTask.SetResult(payload);
            } else {
                _log.Warn(() => LogEvent.Create($"Unexpected Response from {Endpoint} with CorrelationId {correlationId} (not in request queue)."));
            }
        }

        private const int OverflowGuard = int.MaxValue >> 1;
        private int NextCorrelationId()
        {
            var id = Interlocked.Increment(ref _correlationIdSeed);
            if (id > OverflowGuard) {
                // to avoid overflow
                Interlocked.Exchange(ref _correlationIdSeed, 0);
            }
            return id;
        }

        private void AddAsyncRequestItemToResponseQueue(AsyncRequestItem requestItem)
        {
            if (requestItem == null) return;
            if (_requestsByCorrelation.TryAdd(requestItem.CorrelationId, requestItem) == false) {
                throw new ApplicationException("Failed to register request for async response.");
            }
        }

        private void TriggerMessageTimeout(AsyncRequestItem asyncRequestItem)
        {
            if (asyncRequestItem == null) return;

            AsyncRequestItem request;
            _requestsByCorrelation.TryRemove(asyncRequestItem.CorrelationId, out request);

            if (_disposeToken.IsCancellationRequested) {
                asyncRequestItem.ReceiveTask.TrySetException(new ObjectDisposedException("The object is being disposed and the connection is closing."));
            } else {
                asyncRequestItem.ReceiveTask.TrySetException(new TimeoutException($"Timeout expired after {asyncRequestItem.Timeout.TotalMilliseconds} ms."));
            }
        }

        public void Dispose()
        {
            //skip multiple calls to dispose
            if (Interlocked.Increment(ref _disposeCount) != 1) return;

            _disposeToken.Cancel();
            _readerTask?.Wait(TimeSpan.FromSeconds(1));

            using (_disposeToken) {
                using (_socket) {
                }
            }
        }

        private class AsyncRequestItem : IDisposable
        {
            private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();

            public AsyncRequestItem(int correlationId, TimeSpan timeout)
            {
                CorrelationId = correlationId;
                Timeout = timeout;
                ReceiveTask = new TaskCompletionSource<byte[]>();
            }

            public int CorrelationId { get; }
            public TimeSpan Timeout { get; }
            public TaskCompletionSource<byte[]> ReceiveTask { get; }

            public void MarkRequestAsSent(ExceptionDispatchInfo exceptionDispatchInfo, Action<AsyncRequestItem> timeoutFunction)
            {
                if (exceptionDispatchInfo != null) {
                    ReceiveTask.TrySetException(exceptionDispatchInfo.SourceException);
                    exceptionDispatchInfo.Throw();
                }

                _cancellationTokenSource.CancelAfter(Timeout);
                _cancellationTokenSource.Token.Register(() => timeoutFunction(this));
            }

            public void Dispose()
            {
                _cancellationTokenSource.Cancel();
                _cancellationTokenSource.Dispose();
            }
        }
    }
}