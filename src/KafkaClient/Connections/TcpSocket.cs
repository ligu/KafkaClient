using System;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using Nito.AsyncEx;

namespace KafkaClient.Connections
{
    /// <summary>
    /// The TcpSocket provides an abstraction from the main driver from having to handle connection to and reconnections with a server.
    /// The interface is intentionally limited to only read/write.  All connection and reconnect details are handled internally.
    /// </summary>
    public class TcpSocket : ITcpSocket
    {
        private readonly CancellationTokenSource _disposeToken = new CancellationTokenSource();
        private readonly CancellationTokenRegistration _disposeRegistration;
        private int _disposeCount;

        private readonly ILog _log;
        private readonly IConnectionConfiguration _configuration;

        private readonly AsyncCollection<SocketPayloadSendTask> _sendTaskQueue;
        private readonly AsyncCollection<SocketPayloadReceiveTask> _receiveTaskQueue;
        // ReSharper disable NotAccessedField.Local
        private readonly Task _socketTask;
        // ReSharper restore NotAccessedField.Local
        private readonly AsyncLock _clientLock = new AsyncLock();
        private TcpClient _client;

        /// <summary>
        /// Construct socket and open connection to a specified server.
        /// </summary>
        /// <param name="configuration">Configuration for timeouts and retries.</param>
        /// <param name="log">Logging facility for verbose messaging of actions.</param>
        /// <param name="endpoint">The IP endpoint to connect to.</param>
        public TcpSocket(Endpoint endpoint, IConnectionConfiguration configuration = null, ILog log = null)
        {
            Endpoint = endpoint;
            _log = log ?? TraceLog.Log;
            _configuration = configuration ?? new ConnectionConfiguration();
            _sendTaskQueue = new AsyncCollection<SocketPayloadSendTask>();
            _receiveTaskQueue = new AsyncCollection<SocketPayloadReceiveTask>();

            _disposeRegistration = _disposeToken.Token.Register(() => {
                _sendTaskQueue.CompleteAdding();
                _receiveTaskQueue.CompleteAdding();
            });

            // dedicate a long running task to the read/write operations
            _socketTask = Task.Run(DedicatedSocketTask);
        }

        #region Interface Implementation...

        /// <inheritdoc />
        public Endpoint Endpoint { get; }

        /// <inheritdoc />
        public Task<byte[]> ReadAsync(int readSize, CancellationToken cancellationToken)
        {
            var readTask = new SocketPayloadReceiveTask(readSize, cancellationToken);
            _receiveTaskQueue.Add(readTask, cancellationToken);
            return readTask.Tcs.Task;
        }

        /// <inheritdoc />
        public Task<DataPayload> WriteAsync(DataPayload payload, CancellationToken cancellationToken)
        {
            var sendTask = new SocketPayloadSendTask(payload, cancellationToken);
            _sendTaskQueue.Add(sendTask, cancellationToken);
            _configuration.OnWriteEnqueued?.Invoke(Endpoint, payload);
            return sendTask.Tcs.Task;
        }

        #endregion Interface Implementation...

        /// <summary>
        /// Stop all pendding task when can not establish connection in max retry,
        /// but keep trying to recove and connect to this connection.
        /// Only the broker router can dispose of it.
        /// </summary>
        /// <returns></returns>
        private async Task DedicatedSocketTask()
        {
            while (!_disposeToken.IsCancellationRequested) {
                // block here until we can get connections then start loop pushing data through network stream
                try {
                    var netStreamTask = GetStreamAsync();
                    if (await netStreamTask.IsCancelled(_disposeToken.Token).ConfigureAwait(false)) {
                        var disposedException = new ObjectDisposedException($"Object is disposing (TcpSocket for {Endpoint})");
                        await SetExceptionToAllPendingTasksAsync(disposedException);
                        _configuration.OnDisconnected?.Invoke(Endpoint, disposedException);
                        return;
                    }

                    var netStream = await netStreamTask.ConfigureAwait(false);
                    await ProcessNetworkstreamTasks(netStream).ConfigureAwait(false);
                } catch (Exception ex) {
                    await SetExceptionToAllPendingTasksAsync(ex);
                    _configuration.OnDisconnected?.Invoke(Endpoint, ex);
                }
            }
        }

        private async Task SetExceptionToAllPendingTasksAsync(Exception ex)
        {
            var wrappedException = WrappedException(ex);
            var cancelledAny = await _sendTaskQueue.TryApplyAsync(p => p.Tcs.TrySetException(wrappedException), new CancellationToken(true));
            await _receiveTaskQueue.TryApplyAsync(p => p.Tcs.TrySetException(wrappedException), new CancellationToken(true));
            if (cancelledAny) {
                _log.Error(LogEvent.Create(ex, "TcpSocket received an exception, cancelled all pending tasks"));
            }
        }

        private async Task ProcessNetworkstreamTasks(Stream netStream)
        {
            //reading/writing from network steam is not thread safe
            //Read and write operations can be performed simultaneously on an instance of the NetworkStream class without the need for synchronization.
            //As long as there is one unique thread for the write operations and one unique thread for the read operations, there will be no cross-interference
            //between read and write threads and no synchronization is required.
            //https://msdn.microsoft.com/en-us/library/z2xae4f4.aspx

            //Exception need to thrown immediately and not depend on the next task
            var receiveTask = ProcessNetworkstreamTask(netStream, _receiveTaskQueue, ProcessReceiveTaskAsync);
            var sendTask = ProcessNetworkstreamTask(netStream, _sendTaskQueue, ProcessSentTasksAsync);
            await Task.WhenAny(receiveTask, sendTask).ConfigureAwait(false);
            if (_disposeToken.IsCancellationRequested) return;

            await ThrowTaskExceptionIfFaulted(receiveTask);
            await ThrowTaskExceptionIfFaulted(sendTask);
        }

        private static async Task ThrowTaskExceptionIfFaulted(Task task)
        {
            if (task.IsFaulted || task.IsCanceled) await task;
        }

        private async Task ProcessNetworkstreamTask<T>(Stream stream, AsyncCollection<T> queue, Func<Stream, T, Task> asyncProcess)
        {
            Task lastTask = Task.FromResult(true);
            while (_disposeToken.IsCancellationRequested == false && stream != null) {
                await lastTask;
                var takeResult = await queue.TryTakeAsync(_disposeToken.Token);
                if (!takeResult.Success) return;

                lastTask = asyncProcess(stream, takeResult.Item);
            }
        }

        private async Task ProcessReceiveTaskAsync(Stream stream, SocketPayloadReceiveTask receiveTask)
        {
            using (receiveTask) {
                var timer = new Stopwatch();
                try {
                    _configuration.OnReading?.Invoke(Endpoint, receiveTask.ReadSize);
                    var buffer = new byte[receiveTask.ReadSize];
                    timer.Start();
                    for (var totalBytesReceived = 0; totalBytesReceived < receiveTask.ReadSize;) {
                        var readSize = receiveTask.ReadSize - totalBytesReceived;

                        _log.Debug(() => LogEvent.Create($"Receiving data from {Endpoint}, desired size {readSize}"));
                        _configuration.OnReadingChunk?.Invoke(Endpoint, receiveTask.ReadSize, totalBytesReceived, timer.Elapsed);
                        var bytesReceived = await stream.ReadAsync(buffer, totalBytesReceived, readSize, receiveTask.CancellationToken);
                        _configuration.OnReadChunk?.Invoke(Endpoint, receiveTask.ReadSize, receiveTask.ReadSize - totalBytesReceived, bytesReceived, timer.Elapsed);
                        _log.Debug(() => LogEvent.Create($"Received data from {Endpoint}, actual size {bytesReceived}{(receiveTask.CancellationToken.IsCancellationRequested ? " (cancelled)" : "")}"));
                        totalBytesReceived += bytesReceived;

                        if (bytesReceived <= 0) {
                            using (_client) {
                                _client = null;
                                if (_disposeToken.IsCancellationRequested) {
                                    _configuration.OnReadFailed?.Invoke(Endpoint, receiveTask.ReadSize, timer.Elapsed, new TaskCanceledException());
                                    return;
                                }

                                throw new ConnectionException(Endpoint);
                            }
                        }
                    }
                    timer.Stop();
                    _configuration.OnRead?.Invoke(Endpoint, buffer, timer.Elapsed);

                    receiveTask.Tcs.TrySetResult(buffer);
                } catch (Exception ex) {
                    timer.Stop();
                    _configuration.OnReadFailed?.Invoke(Endpoint, receiveTask.ReadSize, timer.Elapsed, ex);

                    if (_disposeToken.IsCancellationRequested) {
                        var exception = new ObjectDisposedException($"Object is disposing (TcpSocket for {Endpoint})");
                        receiveTask.Tcs.TrySetException(exception);
                        throw exception;
                    }

                    if (ex is ConnectionException) {
                        receiveTask.Tcs.TrySetException(ex);
                        if (_disposeToken.IsCancellationRequested) return;
                        throw;
                    }

                    // if an exception made us lose a connection throw disconnected exception
                    if (_client == null || _client.Connected == false) {
                        var exception = new ConnectionException(Endpoint);
                        receiveTask.Tcs.TrySetException(exception);
                        throw exception;
                    }

                    receiveTask.Tcs.TrySetException(ex);
                    if (_disposeToken.IsCancellationRequested) return;

                    throw;
                }
            }
        }

        private async Task ProcessSentTasksAsync(Stream stream, SocketPayloadSendTask sendTask)
        {
            if (sendTask == null) return;

            using (sendTask) {
                var timer = new Stopwatch();
                try {
                    _log.Debug(() => LogEvent.Create($"Sending data to {Endpoint} with CorrelationId {sendTask.Payload.CorrelationId}"));
                    _configuration.OnWriting?.Invoke(Endpoint, sendTask.Payload);
                    timer.Start();
                    await stream.WriteAsync(sendTask.Payload.Buffer, 0, sendTask.Payload.Buffer.Length, _disposeToken.Token);
                    timer.Stop();
                    _configuration.OnWritten?.Invoke(Endpoint, sendTask.Payload, timer.Elapsed);
                    _log.Debug(() => LogEvent.Create($"Sent data to {Endpoint} with CorrelationId {sendTask.Payload.CorrelationId}"));
                    sendTask.Tcs.TrySetResult(sendTask.Payload);
                } catch (Exception ex) {
                    var wrappedException = WrappedException(ex);
                    _configuration.OnWriteFailed?.Invoke(Endpoint, sendTask.Payload, timer.Elapsed, wrappedException);
                    sendTask.Tcs.TrySetException(wrappedException);
                    throw;
                }
            }
        }

        private Exception WrappedException(Exception ex)
        {
            if (_disposeToken.IsCancellationRequested) {
                return new ObjectDisposedException($"Object is disposing (TcpSocket for {Endpoint})");
            }
            return new ConnectionException($"Lost connection to {Endpoint}", ex) { Endpoint = Endpoint };
        }

        private async Task<NetworkStream> GetStreamAsync()
        {
            using (await _clientLock.LockAsync(_disposeToken.Token).ConfigureAwait(false)) {
                if ((_client == null || _client.Connected == false) && !_disposeToken.IsCancellationRequested) {
                    _client = await ReEstablishConnectionAsync().ConfigureAwait(false);
                }

                return _client?.GetStream();
            }
        }

        /// <summary>
        /// (Re-)establish the Kafka server connection.
        /// Assumes that the caller has already obtained the <c>_clientLock</c>
        /// </summary>
        private Task<TcpClient> ReEstablishConnectionAsync()
        {
            _log.Debug(() => LogEvent.Create($"No connection to {Endpoint}: Attempting to connect..."));

            return _configuration.ConnectionRetry.AttemptAsync(
                async (attempt, timer) => {
                    _configuration.OnConnecting?.Invoke(Endpoint, attempt, timer.Elapsed);
                    var client = new TcpClient();

                    var connectTask = client.ConnectAsync(Endpoint.IP.Address, Endpoint.IP.Port);
                    if (await connectTask.IsCancelled(_disposeToken.Token).ConfigureAwait(false)) {
                        throw new ObjectDisposedException($"Object is disposing (TcpSocket for endpoint {Endpoint})");
                    }

                    await connectTask.ConfigureAwait(false);
                    if (!client.Connected) return RetryAttempt<TcpClient>.Failed;

                    _log.Debug(() => LogEvent.Create($"Connection established to {Endpoint}"));
                    _configuration.OnConnected?.Invoke(Endpoint, attempt, timer.Elapsed);
                    return new RetryAttempt<TcpClient>(client);
                },
                (attempt, retry) => _log.Warn(() => LogEvent.Create($"Failed connection to {Endpoint}: Will retry in {retry}")),
                attempt => {
                    _log.Warn(() => LogEvent.Create($"Failed connection to {Endpoint} on attempt {attempt}"));
                    throw new ConnectionException(Endpoint);
                },
                (ex, attempt, retry) => _log.Warn(() => LogEvent.Create(ex, $"Failed connection to {Endpoint}: Will retry in {retry}")),
                (ex, attempt) => _log.Warn(() => LogEvent.Create(ex, $"Failed connection to {Endpoint} on attempt {attempt}")),
                _disposeToken.Token);
        }

        public void Dispose()
        {
            if (Interlocked.Increment(ref _disposeCount) != 1) return;
            _disposeToken.Cancel();

            using (_disposeToken) {
                using (_disposeRegistration) {
                    using (_client) { }
                }
            }
        }
    }
}