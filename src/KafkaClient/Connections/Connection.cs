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

        private readonly SemaphoreSlim _connectSemaphore = new SemaphoreSlim(1, 1);
        private readonly SemaphoreSlim _sendSemaphore = new SemaphoreSlim(1, 1);
        private readonly SemaphoreSlim _versionSupportSemaphore = new SemaphoreSlim(1, 1);
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

            // This thread will poll the receive stream for data, parse a message out
            // and trigger an event with the message payload
            _receiveTask = Task.Factory.StartNew(DedicatedReceiveAsync, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
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

            ArraySegment<byte> receivedBytes;
            using (var asyncItem = new AsyncItem(context, request)) {
                if (request.ExpectResponse) {
                    AddToCorrelationMatching(asyncItem);
                }

                using (var cancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _disposeToken.Token)) {
                    var timer = new Stopwatch();
                    try {
                            await ConnectAsync(cancellation.Token).ConfigureAwait(false);
                            _log.Info(() => LogEvent.Create($"Sending {request.ApiKey} (id {context.CorrelationId}, v {version.GetValueOrDefault()}, {asyncItem.RequestBytes.Count} bytes) to {Endpoint}"));
                            _log.Debug(() => LogEvent.Create($"{request.ApiKey} -----> {Endpoint}\n: Context:{context.ToFormattedString()}\n: Request:{request.ToFormattedString()}"));
                            _configuration.OnWriting?.Invoke(Endpoint, request.ApiKey);
                            timer.Start();
                            var bytesWritten = await WriteBytesAsync(_socket, context.CorrelationId, asyncItem.RequestBytes, cancellationToken).ConfigureAwait(false);
                            timer.Stop();
                            _configuration.OnWritten?.Invoke(Endpoint, request.ApiKey, bytesWritten, timer.Elapsed);

                        if (!request.ExpectResponse) return default (T);
                    } catch (Exception ex) {
                        timer.Stop();
                        _configuration.OnWriteFailed?.Invoke(Endpoint, request.ApiKey, timer.Elapsed, ex);
                        RemoveFromCorrelationMatching(asyncItem, ex);
                        throw;
                    }

                    receivedBytes = await asyncItem.ReceiveTask.Task.ThrowIfCancellationRequested(cancellation.Token).ConfigureAwait(false);
                }
            }

            var response = KafkaEncoder.Decode<T>(context, request.ApiKey, receivedBytes.Skip(KafkaEncoder.CorrelationSize));
            _log.Debug(() => LogEvent.Create($"{Endpoint} -----> {request.ApiKey}\n: Context:{context.ToFormattedString()}\n: Response:{response.ToFormattedString()}"));
            return response;
        }

        private async Task<short> GetVersionAsync(ApiKeyRequestType requestType, CancellationToken cancellationToken)
        {
            if (!_configuration.VersionSupport.IsDynamic) return _configuration.VersionSupport.GetVersion(requestType).GetValueOrDefault();

            var versionSupport = _versionSupport;
            if (versionSupport != null) return versionSupport.GetVersion(requestType).GetValueOrDefault();

            return await _versionSupportSemaphore.LockAsync(
                () => _configuration.ConnectionRetry.AttemptAsync(
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
                    cancellationToken), 
                cancellationToken
            ).ConfigureAwait(false);
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

        private async Task DedicatedReceiveAsync()
        {
            // only allow one reader to execute, dump out all other requests
            if (Interlocked.Increment(ref ActiveReaderCount) != 1) return;

            try {
                var buffer = new byte[8192]; // TODO: configuration on buffer size
                var header = new byte[KafkaEncoder.ResponseHeaderSize];
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
                            responseSize = BitConverter.ToInt32(header, 0).ToBigEndian();
                            // TODO: read correlation id at this point too, and do association before the rest of the read ?
                        }

                        var responseStream = new MemoryStream(responseSize);
                        await ReadBytesAsync(socket, buffer, responseSize, bytesRead => responseStream.Write(buffer, 0, bytesRead), _disposeToken.Token).ConfigureAwait(false);
                        responseSize = 0; // reset so we read the size next time through

                        ArraySegment<byte> response;
                        if (!responseStream.TryGetBuffer(out response)) {
                            response = new ArraySegment<byte>(responseStream.ToArray());
                        }
                        CorrelatePayloadToRequest(response);
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
                DisposeFinal();
                Interlocked.Decrement(ref ActiveReaderCount);
                _log.Info(() => LogEvent.Create($"Stopped receiving from {Endpoint}"));
            }
        }

        protected void DisposeFinal()
        {
            using (_disposeToken) {
                DisposeSocket(_socket);
                _connectSemaphore.Dispose();
                _sendSemaphore.Dispose();
                _versionSupportSemaphore.Dispose();
                _timedOutRequestsByCorrelation.Clear();
            }
        }

        #region Socket

        private Socket CreateSocket()
        {
            if (Endpoint.Value == null) throw new ConnectionException(Endpoint);
            if (_disposeCount > 0) throw new ObjectDisposedException($"Connection to {Endpoint}");

            return new Socket(Endpoint.Value.AddressFamily, SocketType.Stream, ProtocolType.Tcp) {
                Blocking = false,
                SendTimeout = (int)_configuration.RequestTimeout.TotalMilliseconds,
                ReceiveTimeout = (int)_configuration.RequestTimeout.TotalMilliseconds,
                NoDelay = true,
                SendBufferSize = 8092, // Add to configuration?
                ReceiveBufferSize = 8092 // Add to configuration?
            };
        }

        private void DisposeSocket(Socket socket)
        {
            try {
                if (socket == null) return;
                _log.Info(() => LogEvent.Create($"Disposing connection to {Endpoint}"));
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

            var existing = _socket;
            if (existing?.Connected ?? cancellationToken.IsCancellationRequested) return existing;

            return await _connectSemaphore.LockAsync(
                async () => {
                    if (_socket?.Connected ?? cancellationToken.IsCancellationRequested) return _socket;
                    var socket = _socket ?? CreateSocket();
                    _socket = await _configuration.ConnectionRetry.AttemptAsync(
                        async (attempt, timer) => {
                            _log.Info(() => LogEvent.Create($"Connecting to {Endpoint}"));
                            _configuration.OnConnecting?.Invoke(Endpoint, attempt, timer.Elapsed);

                            var connectTask = socket.ConnectAsync(Endpoint.Value.Address, Endpoint.Value.Port);
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
                }, cancellationToken).ConfigureAwait(false);
        }

        internal async Task<int> ReadBytesAsync(Socket socket, byte[] buffer, int bytesToRead, Action<int> onBytesRead, CancellationToken cancellationToken)
        {
            var timer = new Stopwatch();
            var totalBytesRead = 0;
            var cancellation = CancellationTokenSource.CreateLinkedTokenSource(_disposeToken.Token, cancellationToken);
            try {
                _configuration.OnReading?.Invoke(Endpoint, bytesToRead);
                timer.Start();
                while (totalBytesRead < bytesToRead && !cancellation.Token.IsCancellationRequested) {
                    var bytesRemaining = bytesToRead - totalBytesRead;
                    _log.Debug(() => LogEvent.Create($"Reading ({bytesRemaining}? bytes) from {Endpoint}"));
                    _configuration.OnReadingBytes?.Invoke(Endpoint, bytesRemaining);
                    var bytes = new ArraySegment<byte>(buffer, 0, Math.Min(buffer.Length, bytesRemaining));
                    var bytesRead = await socket.ReceiveAsync(bytes, SocketFlags.None).ThrowIfCancellationRequested(cancellation.Token).ConfigureAwait(false);
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
                if (_disposeToken.IsCancellationRequested) throw new ObjectDisposedException(nameof(Connection));
                throw;
            } finally {
                cancellation.Dispose();
            }
            return totalBytesRead;
        }

        internal async Task<int> WriteBytesAsync(Socket socket, int correlationId, ArraySegment<byte> buffer, CancellationToken cancellationToken)
        {
            var totalBytesWritten = 0;
            var timer = new Stopwatch();
            await _sendSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            try {
                timer.Start();
                while (totalBytesWritten < buffer.Count) {
                    _disposeToken.Token.ThrowIfCancellationRequested();
                    cancellationToken.ThrowIfCancellationRequested();

                    var bytesRemaining = buffer.Count - totalBytesWritten;
                    _log.Debug(() => LogEvent.Create($"Writing {bytesRemaining}? bytes (id {correlationId}) to {Endpoint}"));
                    _configuration.OnWritingBytes?.Invoke(Endpoint, bytesRemaining);
                    var bytesWritten = await socket.SendAsync(new ArraySegment<byte>(buffer.Array, buffer.Offset + totalBytesWritten, bytesRemaining), SocketFlags.None).ConfigureAwait(false);
                    _configuration.OnWroteBytes?.Invoke(Endpoint, bytesRemaining, bytesWritten, timer.Elapsed);
                    _log.Debug(() => LogEvent.Create($"Wrote {bytesWritten} bytes (id {correlationId}) to {Endpoint}"));
                    totalBytesWritten += bytesWritten;
                }
            } catch (Exception) {
                if (_disposeToken.IsCancellationRequested) throw new ObjectDisposedException(nameof(Connection));
                throw;
            } finally {
                _sendSemaphore.Release(1);
            }
            return totalBytesWritten;
        }

        #endregion

        #region Correlation

        private void CorrelatePayloadToRequest(ArraySegment<byte> bytes)
        {
            if (bytes.Count < KafkaEncoder.CorrelationSize) return;
            var correlationId = BitConverter.ToInt32(bytes.Array, bytes.Offset).ToBigEndian();

            AsyncItem asyncItem;
            if (_requestsByCorrelation.TryRemove(correlationId, out asyncItem)) {
                var requestType = asyncItem.RequestType;
                var context = asyncItem.Context;
                _log.Info(() => LogEvent.Create($"Matched {requestType} response (id {correlationId}, v {context.ApiVersion.GetValueOrDefault()}, {bytes.Count} bytes) from {Endpoint}"));
                asyncItem.ReceiveTask.TrySetResult(bytes);
            } else {
                Tuple<ApiKeyRequestType, IRequestContext> requestInfo;
                if (_timedOutRequestsByCorrelation.TryRemove(correlationId, out requestInfo)) {
                    var requestType = requestInfo.Item1;
                    var context = requestInfo.Item2;
                    try {
                        _log.Warn(() => LogEvent.Create($"Unexpected {requestType} response (id {correlationId}, {bytes.Count} bytes) from {Endpoint}"));
                        var result = KafkaEncoder.Decode<IResponse>(context, requestType, bytes.Skip(KafkaEncoder.CorrelationSize));
                        _log.Debug(() => LogEvent.Create($"{Endpoint} -----> {requestType} (not in request queue)\n: Context:{context.ToFormattedString()}\n: Response:{result.ToFormattedString()}"));
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
            asyncItem.OnTimedOut(RemoveFromCorrelationMatching, _configuration.RequestTimeout);
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
                asyncItem.ReceiveTask.TrySetException(new TimeoutException($"Timeout expired after {_configuration.RequestTimeout.TotalMilliseconds} ms."));
            }
        }

        #endregion

        public virtual void Dispose()
        {
            // skip multiple calls to dispose
            if (Interlocked.Increment(ref _disposeCount) != 1) return;

            _disposeToken.Cancel(); // other final cleanup taken care of in DedicatedReceiveAsync
        }

        private class AsyncItem : IDisposable
        {
            private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
            private CancellationTokenRegistration _registration;

            public AsyncItem(IRequestContext context, IRequest request)
            {
                Context = context;
                Request = request;
                RequestBytes = KafkaEncoder.Encode(context, request);
                RequestType = request.ApiKey;
                ReceiveTask = new TaskCompletionSource<ArraySegment<byte>>();
            }

            public IRequestContext Context { get; }
            public IRequest Request { get; }
            public ApiKeyRequestType RequestType { get; }
            public ArraySegment<byte> RequestBytes { get; }
            public TaskCompletionSource<ArraySegment<byte>> ReceiveTask { get; }

            public void OnTimedOut(Action<AsyncItem, Exception> cleanupFunction, TimeSpan timeout)
            {
                _registration = _cancellationTokenSource.Token.Register(() => cleanupFunction(this, null));
                _cancellationTokenSource.CancelAfter(timeout);
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