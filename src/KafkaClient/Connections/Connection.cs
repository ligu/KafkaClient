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
        private readonly ConcurrentDictionary<int, Tuple<ApiKeyRequestType, IRequestContext>> _timedOutRequestsByCorrelation = new ConcurrentDictionary<int, Tuple<ApiKeyRequestType, IRequestContext>>();
        private readonly ILog _log;
        private readonly ITcpSocket _socket;
        private readonly IConnectionConfiguration _configuration;

        private readonly CancellationTokenSource _disposeToken = new CancellationTokenSource();
        private int _disposeCount;

        private readonly Task _receiveTask;
        private int _activeReaderCount;
        private static int _correlationIdSeed;

        private readonly AsyncReaderWriterLock _versionSupportLock = new AsyncReaderWriterLock();
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

            // This thread will poll the receive stream for data, parse a message out
            // and trigger an event with the message payload
            _receiveTask = Task.Factory.StartNew(DedicatedReceiveTask, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
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
                version = await GetVersionAsync(request.ApiKey, token).ConfigureAwait(false);
            }
            context = new RequestContext(NextCorrelationId(), version, context?.ClientId, context?.Encoders ?? _configuration.Encoders, context?.ProtocolType, context?.OnProduceRequestMessages ?? _configuration.OnProduceRequestMessages);

            var payload = KafkaEncoder.Encode(context, request);
            _log.Info(() => LogEvent.Create($"Sending {request.ApiKey} with correlation id {context.CorrelationId} (v {version.GetValueOrDefault()}, {payload.Buffer.Length} bytes) to {Endpoint}"));
            _log.Debug(() => LogEvent.Create($"{request.ApiKey} -----> {Endpoint} with correlation id {context.CorrelationId}\n{request.ToFormattedString()}"));
            if (!request.ExpectResponse) {
                await _socket.WriteAsync(payload, token).ConfigureAwait(false);
                return default(T);
            }

            using (var asyncRequest = new AsyncRequestItem(context, request.ApiKey, _configuration.RequestTimeout)) {
                try {
                    AddAsyncRequestItemToResponseQueue(asyncRequest);
                    ExceptionDispatchInfo exceptionDispatchInfo = null;

                    try {
                        await _socket.WriteAsync(payload, token).ConfigureAwait(false);
                    } catch (Exception ex) {
                        exceptionDispatchInfo = ExceptionDispatchInfo.Capture(ex);
                    }

                    asyncRequest.MarkRequestAsSent(exceptionDispatchInfo, TriggerMessageTimeout);
                } catch (OperationCanceledException) {
                    TriggerMessageTimeout(asyncRequest);
                }

                return (T)await asyncRequest.ReceiveTask.Task.ThrowIfCancellationRequested(token).ConfigureAwait(false);
            }
        }

        private async Task<short> GetVersionAsync(ApiKeyRequestType requestType, CancellationToken cancellationToken)
        {
            if (!_configuration.VersionSupport.IsDynamic) return _configuration.VersionSupport.GetVersion(requestType).GetValueOrDefault();

            using (await _versionSupportLock.ReaderLockAsync(cancellationToken).ConfigureAwait(false)) {
                if (_versionSupport != null) return _versionSupport.GetVersion(requestType).GetValueOrDefault();
            }
            using (await _versionSupportLock.WriterLockAsync(cancellationToken).ConfigureAwait(false)) {
                return await _configuration.ConnectionRetry.AttemptAsync(
                    async (attempt, timer) => {
                        var response = await SendAsync(new ApiVersionsRequest(), cancellationToken, new RequestContext(version: 0)).ConfigureAwait(false);
                        if (response.ErrorCode.IsRetryable()) return RetryAttempt<short>.Retry;
                        if (!response.ErrorCode.IsSuccess()) return RetryAttempt<short>.Abort;

                        var supportedVersions = response.SupportedVersions.ToImmutableDictionary(
                                                        _ => _.ApiKey,
                                                        _ => _.MaxVersion);
                        _versionSupport = new VersionSupport(supportedVersions);
                        return new RetryAttempt<short>(_versionSupport.GetVersion(requestType).GetValueOrDefault());
                    },
                    (attempt, timer) => _log.Debug(() => LogEvent.Create($"Retrying {nameof(GetVersionAsync)} attempt {attempt}")),
                    attempt => _versionSupport = _configuration.VersionSupport,
                    exception => _log.Error(LogEvent.Create(exception)),
                    cancellationToken);
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

        private async Task DedicatedReceiveTask()
        {
            try {
                // only allow one reader to execute, dump out all other requests
                if (Interlocked.Increment(ref _activeReaderCount) != 1) return;

                var bytesToSkip = 0;
                var messageSize = 0;
                // use backoff so we don't take over the CPU when there's a failure
                await new BackoffRetry(null, TimeSpan.FromMilliseconds(5), maxDelay: TimeSpan.FromSeconds(5)).AttemptAsync(
                    async attempt => {
                        _log.Debug(() => LogEvent.Create($"Awaiting data from {_socket.Endpoint}"));
                        if (bytesToSkip > 0) {
                            var skipSize = bytesToSkip;
                            _log.Warn(() => LogEvent.Create($"Skipping {skipSize} bytes on {_socket.Endpoint} because of partial read"));
                            await _socket.ReadAsync(skipSize, _disposeToken.Token).ConfigureAwait(false);
                            bytesToSkip = 0;
                        }

                        if (messageSize == 0) {
                            var messageSizeResult = await _socket.ReadAsync(4, _disposeToken.Token).ConfigureAwait(false);
                            messageSize = messageSizeResult.ToInt32(); // hold onto it in case we fail while reading from the socket
                        }

                        var currentSize = messageSize;
                        _log.Debug(() => LogEvent.Create($"Receiving {currentSize} bytes from tcp {_socket.Endpoint}"));
                        var message = await _socket.ReadAsync(currentSize, _disposeToken.Token).ConfigureAwait(false);
                        messageSize = 0; // reset so we read the size next time through

                        CorrelatePayloadToRequest(message);
                        if (attempt > 0) {
                            _log.Info(() => LogEvent.Create($"Polling receive thread has recovered on {_socket.Endpoint}"));
                        }
                    },
                    (exception, attempt, delay) => {
                        if (_disposeToken.IsCancellationRequested) {
                            throw exception.PrepareForRethrow();
                        }

                        var partialException = exception as PartialReadException;
                        if (partialException != null) {
                            if (messageSize > 0) {
                                _log.Warn(() => LogEvent.Create($"Polling failure on {_socket.Endpoint} receive {partialException.BytesRead} of {partialException.ReadSize}"));
                                bytesToSkip = partialException.ReadSize - partialException.BytesRead;
                                messageSize = 0;
                            }
                            exception = partialException.InnerException;
                        }

                        // It is possible for orphaned requests to exist in the _requestsByCorrelation after failure
                        if (attempt == 0) {
                            _log.Error(LogEvent.Create(exception,  $"Polling failure on {_socket.Endpoint} attempt {attempt} delay {delay}"));
                        } else {
                            _log.Info(() => LogEvent.Create(exception, $"Polling failure on {_socket.Endpoint} attempt {attempt} delay {delay}"));
                        }
                    },
                    null, // since there is no max attempts/delay
                    _disposeToken.Token
                ).ConfigureAwait(false);
            } catch (Exception ex) {
                _log.Debug(() => LogEvent.Create(ex));
            } finally {
                Interlocked.Decrement(ref _activeReaderCount);
                _log.Info(() => LogEvent.Create($"Closed down connection to {_socket.Endpoint}"));
            }
        }

        private void CorrelatePayloadToRequest(byte[] payload)
        {
            var correlationId = payload.Take(4).ToArray().ToInt32();

            AsyncRequestItem asyncRequest;
            if (_requestsByCorrelation.TryRemove(correlationId, out asyncRequest)) {
                var requestType = asyncRequest.RequestType;
                var context = asyncRequest.Context;
                _log.Info(() => LogEvent.Create($"Matched {requestType} response with correlation id {correlationId} (v {context.ApiVersion.GetValueOrDefault()}, {payload.Length} bytes) from {Endpoint}"));
                try {
                    var result = KafkaEncoder.Decode<IResponse>(asyncRequest.Context, requestType, payload);
                    _log.Debug(() => LogEvent.Create($"{Endpoint} -----> {requestType} with correlation id {context.CorrelationId}\n{result.ToFormattedString()}"));
                    asyncRequest.ReceiveTask.SetResult(result);
                } catch (Exception ex) {
                    asyncRequest.ReceiveTask.TrySetException(ex);
                }
            } else {
                Tuple<ApiKeyRequestType, IRequestContext> requestInfo;
                if (_timedOutRequestsByCorrelation.TryRemove(correlationId, out requestInfo)) {
                    var requestType = requestInfo.Item1;
                    var context = requestInfo.Item2;
                    try {
                        _log.Warn(() => LogEvent.Create($"Unexpected {requestType} response with correlation id {correlationId} ({payload.Length} bytes) from {Endpoint}"));
                        var result = KafkaEncoder.Decode<IResponse>(context, requestType, payload);
                        _log.Debug(() => LogEvent.Create($"{Endpoint} -----> {requestType} with correlation id {context.CorrelationId} (not in request queue)\n{result.ToFormattedString()}"));
                    } catch {
                        // ignore, since this is mostly for debugging
                    }
                } else {
                    _log.Warn(() => LogEvent.Create($"Unexpected response from {Endpoint} with correlation id {correlationId}"));
                }
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
            if (_requestsByCorrelation.TryAdd(requestItem.Context.CorrelationId, requestItem) == false) {
                throw new KafkaException("Failed to register request for async response.");
            }
        }

        private void TriggerMessageTimeout(AsyncRequestItem asyncRequest)
        {
            if (asyncRequest == null) return;

            var correlationId = asyncRequest.Context.CorrelationId;
            AsyncRequestItem request;
            if (_requestsByCorrelation.TryRemove(correlationId, out request)) {
                _log.Info(() => LogEvent.Create($"Removed request {request.RequestType} with correlation id {correlationId} from request queue (timed out)."));
                if (_timedOutRequestsByCorrelation.Count > 100) {
                    _timedOutRequestsByCorrelation.Clear();
                }
                _timedOutRequestsByCorrelation.TryAdd(correlationId, new Tuple<ApiKeyRequestType, IRequestContext>(request.RequestType, request.Context));
            }

            if (_disposeToken.IsCancellationRequested) {
                asyncRequest.ReceiveTask.TrySetException(new ObjectDisposedException("The object is being disposed and the connection is closing."));
            } else {
                asyncRequest.ReceiveTask.TrySetException(new TimeoutException($"Timeout expired after {asyncRequest.Timeout.TotalMilliseconds} ms."));
            }
        }

        public void Dispose()
        {
            //skip multiple calls to dispose
            if (Interlocked.Increment(ref _disposeCount) != 1) return;

            _disposeToken.Cancel();
            Task.WaitAny(_receiveTask, Task.Delay(TimeSpan.FromSeconds(1)));

            using (_disposeToken) {
                using (_socket) {
                }
            }
        }

        private class AsyncRequestItem : IDisposable
        {
            private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
            private CancellationTokenRegistration _registration;

            public AsyncRequestItem(IRequestContext context, ApiKeyRequestType requestType, TimeSpan timeout)
            {
                Context = context;
                Timeout = timeout;
                RequestType = requestType;
                ReceiveTask = new TaskCompletionSource<IResponse>();
            }

            public IRequestContext Context { get; }
            public ApiKeyRequestType RequestType { get; }
            public TimeSpan Timeout { get; }
            public TaskCompletionSource<IResponse> ReceiveTask { get; }

            public void MarkRequestAsSent(ExceptionDispatchInfo exceptionDispatchInfo, Action<AsyncRequestItem> timeoutFunction)
            {
                if (exceptionDispatchInfo != null) {
                    ReceiveTask.TrySetException(exceptionDispatchInfo.SourceException);
                    exceptionDispatchInfo.Throw();
                }

                _registration = _cancellationTokenSource.Token.Register(() => timeoutFunction(this));
                _cancellationTokenSource.CancelAfter(Timeout);
            }

            public void Dispose()
            {
                using (_cancellationTokenSource) {
                    using (_registration) {
                    }
                }
            }
        }
    }
}