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
        private readonly ConcurrentDictionary<int, AsyncItem> _timedOutRequestsByCorrelation = new ConcurrentDictionary<int, AsyncItem>();
        private readonly ILog _log;
        private readonly ITransport _transport;
        private readonly IConnectionConfiguration _configuration;

        private readonly CancellationTokenSource _disposeToken = new CancellationTokenSource();
        private int _disposeCount; // = 0
        private readonly TaskCompletionSource<bool> _disposePromise = new TaskCompletionSource<bool>();
        public bool IsDisposed => _disposeCount > 0;

        private readonly Task _receiveTask;
        protected int ActiveReaderCount;
        private static int _correlationIdSeed;

        private readonly SemaphoreSlim _versionSupportSemaphore = new SemaphoreSlim(1, 1);
        private IVersionSupport _versionSupport; // = null

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

            _transport = new StreamTransport(_log, _configuration, _disposeToken, endpoint);

            // This thread will poll the receive stream for data, parse a message out
            // and trigger an event with the message payload
            _receiveTask = Task.Factory.StartNew(DedicatedReceiveAsync, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }

        /// <summary>
        /// Indicates a thread is polling the stream for data to read.
        /// </summary>
        internal bool IsReaderAlive => ActiveReaderCount >= 1;

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
                        await _transport.ConnectAsync(cancellation.Token).ConfigureAwait(false);
                        var item = asyncItem;
                        _log.Info(() => LogEvent.Create($"Sending {request.ShortString()} (id {context.CorrelationId}, {item.RequestBytes.Count} bytes) to {Endpoint}"));
                        _log.Debug(() => LogEvent.Create($"{request.ApiKey} -----> {Endpoint} {{Context:{context},\nRequest:{request}}}"));
                        _configuration.OnWriting?.Invoke(Endpoint, request.ApiKey);
                        timer.Start();
                        var bytesWritten = await _transport.WriteBytesAsync(context.CorrelationId, asyncItem.RequestBytes, cancellation.Token).ConfigureAwait(false);
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

            var response = KafkaEncoder.Decode<T>(context, request.ApiKey, receivedBytes);
            _log.Debug(() => LogEvent.Create($"{Endpoint} -----> {{Context:{context},\n{request.ApiKey}Response:{response}}}"));
            return response;
        }

        private async Task<short> GetVersionAsync(ApiKey apiKey, CancellationToken cancellationToken)
        {
            var configuredSupport = _configuration.VersionSupport as DynamicVersionSupport;
            if (configuredSupport == null) return _configuration.VersionSupport.GetVersion(apiKey).GetValueOrDefault(); 

            var versionSupport = _versionSupport;
            if (versionSupport != null) return versionSupport.GetVersion(apiKey).GetValueOrDefault();

            return await _versionSupportSemaphore.LockAsync(
                () => _configuration.ConnectionRetry.AttemptAsync(
                    async (attempt, timer) => {
                        var response = await SendAsync(new ApiVersionsRequest(), cancellationToken, new RequestContext(version: 0)).ConfigureAwait(false);
                        if (response.ErrorCode.IsRetryable()) return RetryAttempt<short>.Retry;
                        if (!response.ErrorCode.IsSuccess()) return RetryAttempt<short>.Abort;

                        var supportedVersions = response.SupportedVersions.ToImmutableDictionary(
                                                        _ => _.ApiKey,
                                                        _ => configuredSupport.UseMaxSupported ? _.MaxVersion : _.MinVersion);
                        _versionSupport = new VersionSupport(supportedVersions);
                        return new RetryAttempt<short>(_versionSupport.GetVersion(apiKey).GetValueOrDefault());
                    },
                    (attempt, timer) => _log.Debug(() => LogEvent.Create($"Retrying {nameof(GetVersionAsync)} attempt {attempt}")),
                    attempt => _versionSupport = _configuration.VersionSupport,
                    exception => _log.Error(LogEvent.Create(exception)),
                    cancellationToken), 
                cancellationToken
            ).ConfigureAwait(false);
        }

        private async Task DedicatedReceiveAsync()
        {
            // only allow one reader to execute, dump out all other requests
            if (Interlocked.Increment(ref ActiveReaderCount) > 1)
            {
                Interlocked.Decrement(ref ActiveReaderCount);
                return;
            }

            try {
                var buffer = new byte[_configuration.ReadBufferSize];
                var header = new byte[KafkaEncoder.ResponseHeaderSize];
                AsyncItem asyncItem = null;
                // use backoff so we don't take over the CPU when there's a failure
                await new BackoffRetry(null, TimeSpan.FromMilliseconds(5), maxDelay: TimeSpan.FromSeconds(5)).AttemptAsync(
                    async attempt => {
                        await _transport.ConnectAsync(_disposeToken.Token).ConfigureAwait(false);

                        if (asyncItem == null) {
                            var headerOffset = 0;
                            await _transport.ReadBytesAsync(buffer, KafkaEncoder.ResponseHeaderSize, bytesRead => {
                                for (var i = 0; i < bytesRead; i++) {
                                    header[headerOffset++] = buffer[i];
                                }
                            }, CancellationToken.None).ConfigureAwait(false);
                            var responseSize = BitConverter.ToInt32(header, 0).ToBigEndian();
                            var correlationId = BitConverter.ToInt32(header, KafkaEncoder.IntegerByteSize).ToBigEndian();

                            asyncItem = LookupByCorrelateId(correlationId, responseSize);
                            if (asyncItem.ResponseStream != null) {
                                _log.Error(LogEvent.Create($"Request id {correlationId} matched a previous response ({asyncItem.ResponseStream.Length + KafkaEncoder.CorrelationSize} of {asyncItem.ResponseStream.Capacity + KafkaEncoder.CorrelationSize} bytes), now overwriting with {responseSize}? bytes"));
                            }
                            asyncItem.ResponseStream = new MemoryStream(responseSize - KafkaEncoder.CorrelationSize);
                        }

                        var currentitem = asyncItem;
                        await _transport.ReadBytesAsync(buffer, asyncItem.RemainingResponseBytes, bytesRead => currentitem.ResponseStream.Write(buffer, 0, bytesRead), CancellationToken.None).ConfigureAwait(false);
                        asyncItem.ResponseCompleted(_log);
                        asyncItem = null;

                        if (attempt > 0) {
                            _log.Info(() => LogEvent.Create($"Polling receive thread has recovered on {Endpoint}"));
                        }
                    },
                    (exception, attempt, delay) => {
                        if (_disposeToken.IsCancellationRequested) {
                            throw exception.PrepareForRethrow();
                        }

                        // when reconnecting, will the data continue to stream or start again??
                        //if (exception is ConnectionException) {
                        //    asyncItem = null;
                        //}

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
                await DisposeAsync().ConfigureAwait(false);
                Interlocked.Decrement(ref ActiveReaderCount);
                _log.Info(() => LogEvent.Create($"Stopped receiving from {Endpoint}"));
            }
        }



        #region Correlation

        private AsyncItem LookupByCorrelateId(int correlationId, int expectedBytes)
        {
            AsyncItem asyncItem;
            if (_requestsByCorrelation.TryRemove(correlationId, out asyncItem) || _timedOutRequestsByCorrelation.TryRemove(correlationId, out asyncItem)) {
                _log.Debug(() => LogEvent.Create($"Matched {asyncItem.ApiKey} response (id {correlationId}, {expectedBytes}? bytes) from {Endpoint}"));
                return asyncItem;
            }

            _log.Warn(() => LogEvent.Create($"Unexpected response (id {correlationId}, {expectedBytes}? bytes) from {Endpoint}"));
            return new AsyncItem(new RequestContext(correlationId), new UnknownRequest());
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
                _log.Info(() => LogEvent.Create($"Removed request {request.ApiKey} (id {correlationId}): timed out or otherwise errored in client."));
                if (_timedOutRequestsByCorrelation.Count > 100) {
                    _timedOutRequestsByCorrelation.Clear();
                }
                _timedOutRequestsByCorrelation.TryAdd(correlationId, request);
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

        public async Task DisposeAsync()
        {
            if (Interlocked.Increment(ref _disposeCount) > 1) {
                await _disposePromise.Task;
                return;
            }

            try {
                _log.Debug(() => LogEvent.Create("Disposing Connection"));
                _disposeToken.Cancel();
                await Task.WhenAny(_receiveTask, Task.Delay(1000)).ConfigureAwait(false);

                using (_disposeToken) {
                    _transport.Dispose();
                    _versionSupportSemaphore.Dispose();
                    _timedOutRequestsByCorrelation.Clear();
                }
            } finally {
                _disposePromise.TrySetResult(true);
            }
        }

        public void Dispose()
        {
#pragma warning disable 4014
            // trigger, and set the promise appropriately
            DisposeAsync();
#pragma warning restore 4014
        }

        private class UnknownRequest : IRequest
        {
            public bool ExpectResponse => true;
            public ApiKey ApiKey => ApiKey.ApiVersions;
            public string ShortString() => "Unknown";
        }

        private class AsyncItem : IDisposable
        {
            private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
            private CancellationTokenRegistration _registration;

            public AsyncItem(IRequestContext context, IRequest request)
            {
                Context = context;
                Request = request;
                RequestBytes = request is UnknownRequest ? new ArraySegment<byte>() : KafkaEncoder.Encode(context, request);
                ApiKey = request.ApiKey;
                ReceiveTask = new TaskCompletionSource<ArraySegment<byte>>();
            }

            public IRequestContext Context { get; }
            private IRequest Request { get; } // for debugging
            public ApiKey ApiKey { get; }
            public ArraySegment<byte> RequestBytes { get; }
            public TaskCompletionSource<ArraySegment<byte>> ReceiveTask { get; }
            public MemoryStream ResponseStream { get; set; }
            public int RemainingResponseBytes => ResponseStream.Capacity - (int)ResponseStream.Length;

            public void ResponseCompleted(ILog log)
            {
                ArraySegment<byte> bytes;
                ResponseStream.TryGetBuffer(out bytes);
                if (Request is UnknownRequest) {
                    log.Debug(() => LogEvent.Create($"Received {ResponseStream.Length + KafkaEncoder.CorrelationSize} bytes (id {Context.CorrelationId})"));
                    return;
                }
                log.Info(() => LogEvent.Create($"Received {ApiKey} response (id {Context.CorrelationId}, {ResponseStream.Length + KafkaEncoder.CorrelationSize} bytes)"));
                if (!ReceiveTask.TrySetResult(bytes)) {
                    log.Debug(
                        () => {
                            var result = KafkaEncoder.Decode<IResponse>(Context, ApiKey, bytes);
                            return LogEvent.Create($"Timed out -----> (timed out or otherwise errored in client) {{Context:{Context},\n{ApiKey}Response:{result}}}");
                        });
                }
            }

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