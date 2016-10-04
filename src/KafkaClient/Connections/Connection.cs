using System;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Protocol;
using Nito.AsyncEx;
using Nito.AsyncEx.Synchronous;

namespace KafkaClient.Connections
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
        private readonly ConcurrentDictionary<int, AsyncRequestItem> _requestsByCorrelation = new ConcurrentDictionary<int, AsyncRequestItem>();
        private readonly ILog _log;
        private readonly ITcpSocket _socket;
        private readonly IConnectionConfiguration _configuration;

        private readonly CancellationTokenSource _disposeToken = new CancellationTokenSource();
        private int _disposeCount;

        private Task _readerTask;
        private int _activeReaderCount;
        private int _correlationIdSeed;
        private IVersionSupport _versionSupport;

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
            _versionSupport = _configuration.VersionSupport.IsDynamic ? null : _configuration.VersionSupport;
            StartReader();
        }

        /// <summary>
        /// Indicates a thread is polling the stream for data to read.
        /// </summary>
        public bool IsReaderAlive => _activeReaderCount >= 1;

        /// <inheritdoc />
        public Endpoint Endpoint => _socket.Endpoint;

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
            var version = context?.ApiVersion;
            if (!version.HasValue) {
                version = await GetVersionAsync(request.ApiKey, token);
            }
            context = new RequestContext(NextCorrelationId(), version, context?.ClientId);

            _log.Debug(() => LogEvent.Create($"SendAsync Api {request.ApiKey} version {context.ApiVersion.GetValueOrDefault()} CorrelationId {context.CorrelationId} to {Endpoint}"));
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

        private async Task<short> GetVersionAsync(ApiKeyRequestType requestType, CancellationToken cancellationToken)
        {
            if (_configuration.VersionSupport.IsDynamic) {
                try {
                    var response = await SendAsync(new ApiVersionsRequest(), cancellationToken, new RequestContext(version: 0));
                    // TODO: what if it doesn't respond or it's unknown ?
                    if (response.ErrorCode == ErrorResponseCode.NoError) {
                        var supportedVersions = response.SupportedVersions.ToImmutableDictionary(
                                                       _ => _.ApiKey,
                                                       _ => _.MaxVersion);
                        _versionSupport = new VersionSupport(supportedVersions);
                    }
                } catch (Exception ex) {
                    _log.Error(LogEvent.Create(ex));
                    _versionSupport = _configuration.VersionSupport;
                }
            }
            return _versionSupport
                .GetVersion(requestType)
                .GetValueOrDefault(_configuration
                    .VersionSupport
                    .GetVersion(requestType)
                    .GetValueOrDefault());
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

                    var messageSize = 0;
                    // use backoff so we don't take over the CPU when there's a failure
                    await new BackoffRetry(TimeSpan.MaxValue, TimeSpan.FromMilliseconds(10), maxDelay: TimeSpan.FromSeconds(5)).AttemptAsync(
                        async attempt => {
                            _log.Debug(() => LogEvent.Create($"Awaiting message from {_socket.Endpoint}"));
                            if (messageSize == 0) {
                                var messageSizeResult = await _socket.ReadAsync(4, _disposeToken.Token).ConfigureAwait(false);
                                messageSize = messageSizeResult.ToInt32(); // hold onto it in case we fail while reading from the socket
                            }

                            var currentSize = messageSize;
                            _log.Debug(() => LogEvent.Create($"Received message of size {currentSize} from {_socket.Endpoint}"));
                            var message = await _socket.ReadAsync(currentSize, _disposeToken.Token).ConfigureAwait(false);
                            messageSize = 0; // reset so we read the size next time through -- note that if the ReadAsync reads partially we're still in trouble

                            CorrelatePayloadToRequest(message);
                            if (attempt > 0) {
                                _log.Info(() => LogEvent.Create($"Polling read thread has recovered on {_socket.Endpoint}"));
                            }
                        },
                        (exception, attempt, delay) => {
                            // It is possible for orphaned requests to exist in the _requestsByCorrelation after failure
                            if (!_disposeToken.IsCancellationRequested) {
                                _log.Debug(() => LogEvent.Create(exception, $"Polling failure on {_socket.Endpoint} attempt {attempt} delay {delay}"));
                                if (attempt == 0) {
                                    _log.Error(LogEvent.Create(exception, $"Polling read thread {_socket?.Endpoint}"));
                                }
                            }
                            _disposeToken.Token.ThrowIfCancellationRequested();
                        },
                        null, // since there is no max attempts/delay
                        _disposeToken.Token
                    ).ConfigureAwait(false);
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
            Task.WaitAny(_readerTask, Task.Delay(TimeSpan.FromSeconds(1)));

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