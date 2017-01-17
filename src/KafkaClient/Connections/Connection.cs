using System;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Net.Sockets;
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
        private readonly Socket _socket;
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
        /// <param name="endpoint">The IP endpoint to connect to.</param>
        /// <param name="configuration">The configuration, including connection and request timeouts.</param>
        /// <param name="log">Logging interface used to record any log messages created by the connection.</param>
        public Connection(Endpoint endpoint, IConnectionConfiguration configuration = null, ILog log = null)
        {
            Endpoint = endpoint;
            _configuration = configuration ?? new ConnectionConfiguration();

            _socket = new Socket(Endpoint.IP.AddressFamily, SocketType.Stream, ProtocolType.Tcp) {
                Blocking = false,
                SendTimeout = (int)_configuration.RequestTimeout.TotalMilliseconds,
                SendBufferSize = 8092,
                ReceiveBufferSize = 8092
            };
            _log = log ?? TraceLog.Log;
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
        public Endpoint Endpoint { get; }

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
            context = RequestContext.Copy(context, NextCorrelationId(), version, encoders: _configuration.Encoders, onProduceRequestMessages: _configuration.OnProduceRequestMessages);

            byte[] receivedBytes;
            using (var asyncRequest = new AsyncRequestItem(context, request.ApiKey, KafkaEncoder.Encode(context, request), _configuration.RequestTimeout)) {
                if (request.ExpectResponse) {
                    AddToCorrelationMatching(asyncRequest);
                }

                var timer = new Stopwatch();
                try {
                    _log.Info(() => LogEvent.Create($"Sending {request.ApiKey} with correlation id {context.CorrelationId} (v {version.GetValueOrDefault()}, {KafkaEncoder.Encode(context, request).Buffer.Length} bytes) to {Endpoint}"));
                    _log.Debug(() => LogEvent.Create($"{request.ApiKey} -----> {Endpoint}\n- Context:{context.ToFormattedString()}\n- Request:{request.ToFormattedString()}"));
                    _configuration.OnWriteEnqueued?.Invoke(Endpoint, asyncRequest.SendPayload);

                    var buffer = asyncRequest.SendPayload.Buffer;
                    timer.Start();
                    for (var offset = 0; offset < buffer.Length && !token.IsCancellationRequested; ) {
                        var bytesRemaining = buffer.Length - offset;
                        _log.Debug(() => LogEvent.Create($"Writing ({bytesRemaining}? bytes) with correlation id {context.CorrelationId} to {Endpoint}"));
                        // Chunk? _configuration.OnWriting?.Invoke(Endpoint, asyncRequest.SendPayload);
                        var bytesWritten = await _socket.SendAsync(new ArraySegment<byte>(buffer, offset, bytesRemaining), SocketFlags.None).ConfigureAwait(false);
                        // Chunk? _configuration.OnWritten?.Invoke(Endpoint, asyncRequest.SendPayload, timer.Elapsed);
                        _log.Debug(() => LogEvent.Create($"Wrote {bytesWritten} bytes with correlation id {context.CorrelationId} to {Endpoint}"));
                        offset += bytesWritten;
                    }
                    timer.Stop();
                    _configuration.OnWritten?.Invoke(Endpoint, asyncRequest.SendPayload, timer.Elapsed);

                    if (!request.ExpectResponse) return default (T);
                } catch (Exception ex) {
                    timer.Stop();
                    _configuration.OnWriteFailed?.Invoke(Endpoint, asyncRequest.SendPayload, timer.Elapsed, ex);
                    asyncRequest.ReceiveTask.TrySetException(ex);
                    RemoveFromCorrelationMatching(asyncRequest);
                    throw;
                }

                receivedBytes = await asyncRequest.ReceiveTask.Task.ThrowIfCancellationRequested(token).ConfigureAwait(false);
            }

            var response = KafkaEncoder.Decode<T>(context, request.ApiKey, receivedBytes);
            _log.Debug(() => LogEvent.Create($"{Endpoint} -----> {request.ApiKey}\n- Context:{context.ToFormattedString()}\n- Response:{response.ToFormattedString()}"));
            return response;
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
            return Equals(Endpoint, other.Endpoint);
        }

        public override int GetHashCode()
        {
            return Endpoint?.GetHashCode() ?? 0;
        }

        #endregion Equals

        private async Task DedicatedReceiveTask()
        {
            try {
                // only allow one reader to execute, dump out all other requests
                if (Interlocked.Increment(ref _activeReaderCount) != 1) return;

                var buffer = new byte[8192];
                var bytesToSkip = 0;
                var responseSize = 0;
                // use backoff so we don't take over the CPU when there's a failure
                await new BackoffRetry(null, TimeSpan.FromMilliseconds(5), maxDelay: TimeSpan.FromSeconds(5)).AttemptAsync(
                    async attempt => {
                        if (!_socket.Connected) {
                            _log.Info(() => LogEvent.Create($"Connecting to {Endpoint}"));
                            await _socket.ConnectAsync(Endpoint.IP.Address, Endpoint.IP.Port).ConfigureAwait(false);
                            bytesToSkip = 0;
                        }

                        if (bytesToSkip > 0) {
                            _log.Warn(() => LogEvent.Create($"Skipping {bytesToSkip} bytes on {Endpoint} because of partial read"));
                            await ReadBytesAsync(buffer, bytesToSkip, bytesRead => bytesToSkip -= bytesRead).ConfigureAwait(false);
                        }

                        if (responseSize == 0) {
                            await ReadBytesAsync(buffer, KafkaEncoder.IntegerByteSize, bytesRead => responseSize = buffer.Take(KafkaEncoder.IntegerByteSize).ToInt32()).ConfigureAwait(false);
                        }

                        var response = new MemoryStream(responseSize);
                        await ReadBytesAsync(buffer, responseSize, bytesRead => response.Write(buffer, 0, bytesRead)).ConfigureAwait(false);
                        responseSize = 0; // reset so we read the size next time through

                        CorrelatePayloadToRequest(response.ToArray());
                        if (attempt > 0) {
                            _log.Info(() => LogEvent.Create($"Polling receive thread has recovered on {Endpoint}"));
                        }
                    },
                    (exception, attempt, delay) => {
                        if (_disposeToken.IsCancellationRequested) {
                            throw exception.PrepareForRethrow();
                        }

                        var partialException = exception as PartialReadException;
                        if (partialException != null) {
                            if (responseSize > 0) {
                                _log.Warn(() => LogEvent.Create($"Polling failure on {Endpoint} receive {partialException.BytesRead} of {partialException.ReadSize}"));
                                bytesToSkip = partialException.ReadSize - partialException.BytesRead;
                                responseSize = 0;
                            }
                            exception = partialException.InnerException;
                        } else if (exception is ConnectionException) {
                            // reset bytesToSkip when connection fails (and socket is recreated)
                            bytesToSkip = 0;
                        }

                        if (attempt == 0) {
                            _log.Error(LogEvent.Create(exception,  $"Polling failure on {Endpoint} attempt {attempt} delay {delay}"));
                        } else {
                            _log.Info(() => LogEvent.Create(exception, $"Polling failure on {Endpoint} attempt {attempt} delay {delay}"));
                        }
                    },
                    null, // since there is no max attempts/delay
                    _disposeToken.Token
                ).ConfigureAwait(false);
            } catch (Exception ex) {
                _log.Debug(() => LogEvent.Create(ex));
            } finally {
                Interlocked.Decrement(ref _activeReaderCount);
                _log.Info(() => LogEvent.Create($"Closed down connection to {Endpoint}"));
            }
        }

        private async Task ReadBytesAsync(byte[] buffer, int bytesToRead, Action<int> onBytesRead)
        {
            var timer = new Stopwatch();
            timer.Start();
            for (var totalBytesRead = 0; totalBytesRead < bytesToRead && !_disposeToken.IsCancellationRequested;) {
                var bytesRemaining = bytesToRead - totalBytesRead;
                _log.Debug(() => LogEvent.Create($"Reading ({bytesRemaining}? bytes) from {Endpoint}"));
                _configuration.OnReadingChunk?.Invoke(Endpoint, bytesRemaining, totalBytesRead, timer.Elapsed);
                var bytes = new ArraySegment<byte>(buffer, 0, Math.Min(buffer.Length, bytesRemaining));
                var bytesRead = await _socket.ReceiveAsync(bytes, SocketFlags.None).ConfigureAwait(false);
                totalBytesRead += bytesRead;
                _configuration.OnReadChunk?.Invoke(Endpoint, bytesRemaining, bytesRemaining - bytesRead, bytesRead, timer.Elapsed);
                _log.Debug(() => LogEvent.Create($"Read {bytesRead} bytes from {Endpoint}"));
                onBytesRead(bytesRead);
            }
            timer.Stop();
        }

        private void CorrelatePayloadToRequest(byte[] payload)
        {
            var correlationId = payload.Take(4).ToArray().ToInt32();

            AsyncRequestItem asyncRequest;
            if (_requestsByCorrelation.TryRemove(correlationId, out asyncRequest)) {
                var requestType = asyncRequest.RequestType;
                var context = asyncRequest.Context;
                _log.Info(() => LogEvent.Create($"Matched {requestType} response with correlation id {correlationId} (v {context.ApiVersion.GetValueOrDefault()}, {payload.Length} bytes) from {Endpoint}"));
                asyncRequest.ReceiveTask.SetResult(payload);
            } else {
                Tuple<ApiKeyRequestType, IRequestContext> requestInfo;
                if (_timedOutRequestsByCorrelation.TryRemove(correlationId, out requestInfo)) {
                    var requestType = requestInfo.Item1;
                    var context = requestInfo.Item2;
                    try {
                        _log.Warn(() => LogEvent.Create($"Unexpected {requestType} response with correlation id {correlationId} ({payload.Length} bytes) from {Endpoint}"));
                        var result = KafkaEncoder.Decode<IResponse>(context, requestType, payload);
                        _log.Debug(() => LogEvent.Create($"{Endpoint} -----> {requestType} (not in request queue)\n- Context:{context.ToFormattedString()}\n- Response:{result.ToFormattedString()}"));
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

        private void AddToCorrelationMatching(AsyncRequestItem asyncRequest)
        {
            if (asyncRequest == null) return;
            if (_requestsByCorrelation.TryAdd(asyncRequest.Context.CorrelationId, asyncRequest) == false) {
                throw new KafkaException("Failed to register request for async response.");
            }
            asyncRequest.OnDispose(RemoveFromCorrelationMatching);
        }

        private void RemoveFromCorrelationMatching(AsyncRequestItem asyncRequest)
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
                    _socket.Shutdown(SocketShutdown.Both);
                }
            }
        }

        {
        }

        private class AsyncRequestItem : IDisposable
        {
            private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
            private CancellationTokenRegistration _registration;

            public AsyncRequestItem(IRequestContext context, ApiKeyRequestType requestType, DataPayload sendPayload, TimeSpan timeout)
            {
                Context = context;
                Timeout = timeout;
                SendPayload = sendPayload;
                RequestType = requestType;
                ReceiveTask = new TaskCompletionSource<byte[]>();
            }

            public IRequestContext Context { get; }
            public ApiKeyRequestType RequestType { get; }
            public DataPayload SendPayload { get; }
            public TimeSpan Timeout { get; }
            public TaskCompletionSource<byte[]> ReceiveTask { get; }

            public void OnDispose(Action<AsyncRequestItem> cleanupFunction)
            {
                _registration = _cancellationTokenSource.Token.Register(() => cleanupFunction(this));
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