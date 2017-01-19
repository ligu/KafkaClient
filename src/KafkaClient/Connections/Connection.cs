using System;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Diagnostics;
using System.IO;
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
        private readonly ConcurrentDictionary<int, AsyncItem> _requestsByCorrelation = new ConcurrentDictionary<int, AsyncItem>();
        private readonly ConcurrentDictionary<int, Tuple<ApiKeyRequestType, IRequestContext>> _timedOutRequestsByCorrelation = new ConcurrentDictionary<int, Tuple<ApiKeyRequestType, IRequestContext>>();
        private readonly ILog _log;
        private Socket _socket;
        private readonly IConnectionConfiguration _configuration;

        private readonly CancellationTokenSource _disposeToken = new CancellationTokenSource();
        private int _disposeCount;

        private readonly Task _receiveTask;
        protected int ActiveReaderCount;
        private static int _correlationIdSeed;

        private readonly AsyncReaderWriterLock _socketLock = new AsyncReaderWriterLock();
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

            _log = log ?? TraceLog.Log;
            _versionSupport = _configuration.VersionSupport.IsDynamic ? null : _configuration.VersionSupport;
            _socket = CreateSocket();

            // This thread will poll the receive stream for data, parse a message out
            // and trigger an event with the message payload
            _receiveTask = Task.Factory.StartNew(DedicatedReceiveTask, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }

        /// <summary>
        /// Indicates a thread is polling the stream for data to read.
        /// </summary>
        public bool IsReaderAlive => ActiveReaderCount >= 1;

        /// <inheritdoc />
        public Endpoint Endpoint { get; }

        /// <summary>
        /// Send kafka payload to server and receive a task event when response is received.
        /// </summary>
        /// <typeparam name="T">A Kafka response object return by decode function.</typeparam>
        /// <param name="request">The IRequest to send to the kafka servers.</param>
        /// <param name="context">The context for the request.</param>
        /// <param name="cancellationToken">Cancellation token used to cancel the transfer.</param>
        /// <returns></returns>
        public async Task<T> SendAsync<T>(IRequest<T> request, CancellationToken cancellationToken, IRequestContext context = null) where T : class, IResponse
        {
            var version = context?.ApiVersion;
            if (!version.HasValue) {
                version = await GetVersionAsync(request.ApiKey, cancellationToken).ConfigureAwait(false);
            }
            context = RequestContext.Copy(context, NextCorrelationId(), version, encoders: _configuration.Encoders, onProduceRequestMessages: _configuration.OnProduceRequestMessages);

            byte[] receivedBytes;
            using (var asyncItem = new AsyncItem(context, request.ApiKey, KafkaEncoder.Encode(context, request), _configuration.RequestTimeout)) {
                if (request.ExpectResponse) {
                    AddToCorrelationMatching(asyncItem);
                }

                _log.Info(() => LogEvent.Create($"Sending {request.ApiKey} (id {context.CorrelationId}, v {version.GetValueOrDefault()}, {asyncItem.RequestBytes.Length} bytes) to {Endpoint}"));
                _log.Debug(() => LogEvent.Create($"{request.ApiKey} -----> {Endpoint}\n- Context:{context.ToFormattedString()}\n- Request:{request.ToFormattedString()}"));

                var timer = new Stopwatch();
                try {
                    await ConnectAsync(cancellationToken).ConfigureAwait(false);
                    _configuration.OnWriting?.Invoke(Endpoint, request.ApiKey);
                    timer.Start();
                    var bytesWritten = await WriteBytesAsync(_socket, context.CorrelationId, asyncItem.RequestBytes, cancellationToken);
                    timer.Stop();
                    _configuration.OnWritten?.Invoke(Endpoint, request.ApiKey, bytesWritten, timer.Elapsed);

                    if (!request.ExpectResponse) return default (T);
                } catch (Exception ex) {
                    timer.Stop();
                    _configuration.OnWriteFailed?.Invoke(Endpoint, request.ApiKey, timer.Elapsed, ex);
                    RemoveFromCorrelationMatching(asyncItem, ex);
                    throw;
                }

                receivedBytes = await asyncItem.ReceiveTask.Task.ThrowIfCancellationRequested(cancellationToken).ConfigureAwait(false);
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
                    cancellationToken).ConfigureAwait(false);
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
            // only allow one reader to execute, dump out all other requests
            if (Interlocked.Increment(ref ActiveReaderCount) != 1) return;

            try {
                var buffer = new byte[8192];
                var header = new byte[KafkaEncoder.IntegerByteSize];
                var bytesToSkip = 0;
                var responseSize = 0;
                // use backoff so we don't take over the CPU when there's a failure
                await new BackoffRetry(null, TimeSpan.FromMilliseconds(5), maxDelay: TimeSpan.FromSeconds(5)).AttemptAsync(
                    async attempt => {
                        var socket = await ConnectAsync(_disposeToken.Token).ConfigureAwait(false);

                        if (bytesToSkip > 0) {
                            _log.Warn(() => LogEvent.Create($"Skipping {bytesToSkip} bytes on {Endpoint} because of partial read"));
                            await ReadBytesAsync(socket, buffer, bytesToSkip, bytesRead => bytesToSkip -= bytesRead, _disposeToken.Token).ConfigureAwait(false);
                        }

                        if (responseSize == 0) {
                            var headerOffset = 0;
                            await ReadBytesAsync(socket, buffer, KafkaEncoder.IntegerByteSize, bytesRead =>
                            {
                                Buffer.BlockCopy(buffer, 0, header, headerOffset, bytesRead);
                                headerOffset += bytesRead;
                            }, _disposeToken.Token).ConfigureAwait(false);
                            responseSize = header.ToInt32();
                            // TODO: read correlation id at this point too, and do association before the rest of the read ?
                        }

                        var response = new MemoryStream(responseSize);
                        await ReadBytesAsync(socket, buffer, responseSize, bytesRead => response.Write(buffer, 0, bytesRead), _disposeToken.Token).ConfigureAwait(false);
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
                Interlocked.Decrement(ref ActiveReaderCount);
                _log.Info(() => LogEvent.Create($"Closed down connection to {Endpoint}"));
            }
        }

        #region Socket

        private Socket CreateSocket()
        {
            if (Endpoint.IP == null) throw new ConnectionException(Endpoint);

            return new Socket(Endpoint.IP.AddressFamily, SocketType.Stream, ProtocolType.Tcp) {
                Blocking = false,
                SendTimeout = (int)_configuration.RequestTimeout.TotalMilliseconds,
                SendBufferSize = 8092, // Add to configuration?
                ReceiveBufferSize = 8092 // Add to configuration?
            };
        }

        private void DisposeSocket(Socket socket)
        {
            try {
                if (socket == null) return;
                using (socket) {
                    if (socket.Connected) {
                        socket.Shutdown(SocketShutdown.Both);
                    }
                }
            } catch (Exception ex) {
                _log.Info(() => LogEvent.Create(ex));
            }
        }

        internal async Task<Socket> ConnectAsync(CancellationToken cancellationToken)
        {
            if (_disposeCount > 0) throw new ObjectDisposedException(nameof(Connection));

            using (await _socketLock.ReaderLockAsync(cancellationToken).ConfigureAwait(false)) {
                if (_socket.Connected || cancellationToken.IsCancellationRequested) return _socket;
            }
            using (await _socketLock.WriterLockAsync(cancellationToken).ConfigureAwait(false)) {
                if (_socket.Connected || cancellationToken.IsCancellationRequested) return _socket;
                var socket = _socket;
                _socket = await _configuration.ConnectionRetry.AttemptAsync(
                    async (attempt, timer) => {
                        _log.Info(() => LogEvent.Create($"Connecting to {Endpoint}"));
                        _configuration.OnConnecting?.Invoke(Endpoint, attempt, timer.Elapsed);

                        var connectTask = socket.ConnectAsync(Endpoint.IP.Address, Endpoint.IP.Port);
                        if (await connectTask.IsCancelled(_disposeToken.Token).ConfigureAwait(false)) {
                            throw new ObjectDisposedException($"Object is disposing (TcpSocket for endpoint {Endpoint})");
                        }

                        await connectTask.ConfigureAwait(false);
                        if (!socket.Connected) return RetryAttempt<Socket>.Retry;

                        _log.Info(() => LogEvent.Create($"Connection established to {Endpoint}"));
                        _configuration.OnConnected?.Invoke(Endpoint, attempt, timer.Elapsed);
                        return new RetryAttempt<Socket>(socket);
                    },
                    (attempt, retry) => _log.Warn(() => LogEvent.Create($"Failed connection to {Endpoint}: Will retry in {retry}")),
                    attempt => {
                        _log.Warn(() => LogEvent.Create($"Failed connection to {Endpoint} on attempt {attempt}"));
                        throw new ConnectionException(Endpoint);
                    },
                    (ex, attempt, retry) =>
                    {
                        if (ex is ObjectDisposedException) {
                            if (_disposeToken.IsCancellationRequested) throw ex.PrepareForRethrow();

                            DisposeSocket(socket);
                            _log.Info(() => LogEvent.Create(ex, $"Creating new socket to {Endpoint}"));
                            socket = CreateSocket();
                        }
                        _log.Warn(() => LogEvent.Create(ex, $"Failed connection to {Endpoint}: Will retry in {retry}"));
                    },
                    (ex, attempt) =>
                    {
                        _log.Warn(() => LogEvent.Create(ex, $"Failed connection to {Endpoint} on attempt {attempt}"));
                        if (ex is SocketException) {
                            throw new ConnectionException(Endpoint, ex);
                        }
                    },
                    cancellationToken).ConfigureAwait(false);
                return _socket;
            }
        }

        internal async Task<int> ReadBytesAsync(Socket socket, byte[] buffer, int bytesToRead, Action<int> onBytesRead, CancellationToken cancellationToken)
        {
            var timer = new Stopwatch();
            var totalBytesRead = 0;
            try {
                _configuration.OnReading?.Invoke(Endpoint, bytesToRead);
                timer.Start();
                while (totalBytesRead < bytesToRead && !cancellationToken.IsCancellationRequested) {
                    var bytesRemaining = bytesToRead - totalBytesRead;
                    _log.Debug(() => LogEvent.Create($"Reading ({bytesRemaining}? bytes) from {Endpoint}"));
                    _configuration.OnReadingBytes?.Invoke(Endpoint, bytesRemaining);
                    var bytes = new ArraySegment<byte>(buffer, 0, Math.Min(buffer.Length, bytesRemaining));
                    var bytesRead = await socket.ReceiveAsync(bytes, SocketFlags.None).ThrowIfCancellationRequested(cancellationToken).ConfigureAwait(false);
                    totalBytesRead += bytesRead;
                    _configuration.OnReadBytes?.Invoke(Endpoint, bytesRemaining, bytesRead, timer.Elapsed);
                    _log.Debug(() => LogEvent.Create($"Read {bytesRead} bytes from {Endpoint}"));

                    if (bytesRead <= 0 && socket.Available == 0) {
                        DisposeSocket(socket);
                        Exception ex = new ConnectionException(Endpoint);
                        if (totalBytesRead > 0) ex = new PartialReadException(totalBytesRead, bytesToRead, ex);
                        _configuration.OnDisconnected?.Invoke(Endpoint, ex);
                        throw ex;
                    }
                    onBytesRead(bytesRead);
                }
                timer.Stop();
                _configuration.OnRead?.Invoke(Endpoint, totalBytesRead, timer.Elapsed);
            } catch (Exception ex) {
                timer.Stop();
                _configuration.OnReadFailed?.Invoke(Endpoint, bytesToRead, timer.Elapsed, ex);
                throw;
            }
            return totalBytesRead;
        }

        internal async Task<int> WriteBytesAsync(Socket socket, int correlationId, byte[] buffer, CancellationToken cancellationToken)
        {
            var totalBytesWritten = 0;
            var timer = new Stopwatch();
            timer.Start();
            while (totalBytesWritten < buffer.Length) {
                cancellationToken.ThrowIfCancellationRequested();
                var bytesRemaining = buffer.Length - totalBytesWritten;
                _log.Debug(() => LogEvent.Create($"Writing {bytesRemaining}? bytes (id {correlationId}) to {Endpoint}"));
                _configuration.OnWritingBytes?.Invoke(Endpoint, bytesRemaining);
                var bytesWritten = await socket.SendAsync(new ArraySegment<byte>(buffer, totalBytesWritten, bytesRemaining), SocketFlags.None).ThrowIfCancellationRequested(cancellationToken).ConfigureAwait(false);
                _configuration.OnWroteBytes?.Invoke(Endpoint, bytesRemaining, bytesWritten, timer.Elapsed);
                _log.Debug(() => LogEvent.Create($"Wrote {bytesWritten} bytes (id {correlationId}) to {Endpoint}"));
                totalBytesWritten += bytesWritten;
            }
            return totalBytesWritten;
        }

        #endregion

        #region Correlation

        private void CorrelatePayloadToRequest(byte[] payload)
        {
            if (payload.Length < 4) return;
            var correlationId = payload.ToInt32();

            AsyncItem asyncItem;
            if (_requestsByCorrelation.TryRemove(correlationId, out asyncItem)) {
                var requestType = asyncItem.RequestType;
                var context = asyncItem.Context;
                _log.Info(() => LogEvent.Create($"Matched {requestType} response (id {correlationId}, v {context.ApiVersion.GetValueOrDefault()}, {payload.Length} bytes) from {Endpoint}"));
                asyncItem.ReceiveTask.SetResult(payload);
            } else {
                Tuple<ApiKeyRequestType, IRequestContext> requestInfo;
                if (_timedOutRequestsByCorrelation.TryRemove(correlationId, out requestInfo)) {
                    var requestType = requestInfo.Item1;
                    var context = requestInfo.Item2;
                    try {
                        _log.Warn(() => LogEvent.Create($"Unexpected {requestType} response (id {correlationId}, {payload.Length} bytes) from {Endpoint}"));
                        var result = KafkaEncoder.Decode<IResponse>(context, requestType, payload);
                        _log.Debug(() => LogEvent.Create($"{Endpoint} -----> {requestType} (not in request queue)\n- Context:{context.ToFormattedString()}\n- Response:{result.ToFormattedString()}"));
                    } catch {
                        // ignore, since this is mostly for debugging
                    }
                } else {
                    _log.Warn(() => LogEvent.Create($"Unexpected response (id {correlationId}) from {Endpoint}"));
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

        private void AddToCorrelationMatching(AsyncItem asyncItem)
        {
            if (asyncItem == null) return;
            if (_requestsByCorrelation.TryAdd(asyncItem.Context.CorrelationId, asyncItem) == false) {
                throw new KafkaException("Failed to register request for async response.");
            }
            asyncItem.OnDispose(RemoveFromCorrelationMatching);
        }

        private void RemoveFromCorrelationMatching(AsyncItem asyncItem, Exception exception = null)
        {
            if (asyncItem == null) return;

            var correlationId = asyncItem.Context.CorrelationId;
            AsyncItem request;
            if (_requestsByCorrelation.TryRemove(correlationId, out request)) {
                _log.Info(() => LogEvent.Create($"Removed request {request.RequestType} (id {correlationId}) from request queue (timed out)."));
                if (_timedOutRequestsByCorrelation.Count > 100) {
                    _timedOutRequestsByCorrelation.Clear();
                }
                _timedOutRequestsByCorrelation.TryAdd(correlationId, new Tuple<ApiKeyRequestType, IRequestContext>(request.RequestType, request.Context));
            }

            if (_disposeToken.IsCancellationRequested) {
                asyncItem.ReceiveTask.TrySetException(new ObjectDisposedException("The object is being disposed and the connection is closing."));
            } else if (exception != null) {
                asyncItem.ReceiveTask.TrySetException(exception);
            } else {
                asyncItem.ReceiveTask.TrySetException(new TimeoutException($"Timeout expired after {asyncItem.Timeout.TotalMilliseconds} ms."));
            }
        }

        #endregion

        public void Dispose()
        {
            //skip multiple calls to dispose
            if (Interlocked.Increment(ref _disposeCount) != 1) return;

            _disposeToken.Cancel();
            Task.WaitAny(_receiveTask, Task.Delay(TimeSpan.FromSeconds(1)));

            using (_disposeToken) {
                DisposeSocket(_socket);
                _timedOutRequestsByCorrelation.Clear();
            }
        }

        private class AsyncItem : IDisposable
        {
            private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
            private CancellationTokenRegistration _registration;

            public AsyncItem(IRequestContext context, ApiKeyRequestType requestType, byte [] requestBytes, TimeSpan timeout)
            {
                Context = context;
                Timeout = timeout;
                RequestBytes = requestBytes;
                RequestType = requestType;
                ReceiveTask = new TaskCompletionSource<byte[]>();
            }

            public IRequestContext Context { get; }
            public ApiKeyRequestType RequestType { get; }
            public byte[] RequestBytes { get; }
            public TimeSpan Timeout { get; }
            public TaskCompletionSource<byte[]> ReceiveTask { get; }

            public void OnDispose(Action<AsyncItem, Exception> cleanupFunction)
            {
                _registration = _cancellationTokenSource.Token.Register(() => cleanupFunction(this, null));
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