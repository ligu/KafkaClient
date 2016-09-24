using System;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;

namespace KafkaClient.Connection
{
    /// <summary>
    /// The TcpSocket provides an abstraction from the main driver from having to handle connection to and reconnections with a server.
    /// The interface is intentionally limited to only read/write.  All connection and reconnect details are handled internally.
    /// </summary>
    public class TcpSocket : ITcpSocket
    {
        private readonly CancellationTokenSource _disposeToken = new CancellationTokenSource();
        private readonly CancellationTokenRegistration _disposeRegistration;
        private readonly Task _disposeTask;
        private int _disposeCount;

        private readonly ILog _log;
        private readonly IConnectionConfiguration _configuration;

        private readonly AsyncCollection<SocketPayloadSendTask> _sendTaskQueue;
        private readonly AsyncCollection<SocketPayloadReceiveTask> _readTaskQueue;
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
            _readTaskQueue = new AsyncCollection<SocketPayloadReceiveTask>();

            // dedicate a long running task to the read/write operations
            _socketTask = Task.Run(DedicatedSocketTask);

            _disposeTask = _disposeToken.Token.CreateTask();
            _disposeRegistration = _disposeToken.Token.Register(() => {
                _sendTaskQueue.CompleteAdding();
                _readTaskQueue.CompleteAdding();
            });
        }

        #region Interface Implementation...

        /// <inheritdoc />
        public Endpoint Endpoint { get; }

        /// <inheritdoc />
        public Task<byte[]> ReadAsync(int readSize, CancellationToken cancellationToken)
        {
            var readTask = new SocketPayloadReceiveTask(readSize, cancellationToken);
            _readTaskQueue.Add(readTask);
            return readTask.Tcp.Task;
        }

        /// <inheritdoc />
        public Task<DataPayload> WriteAsync(DataPayload payload, CancellationToken cancellationToken)
        {
            var sendTask = new SocketPayloadSendTask(payload, cancellationToken);
            _sendTaskQueue.Add(sendTask);
            _configuration.OnWriteEnqueued?.Invoke(Endpoint, payload);
            return sendTask.Tcp.Task;
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
                    await Task.WhenAny(_disposeTask, netStreamTask).ConfigureAwait(false);

                    if (_disposeToken.IsCancellationRequested) {
                        var disposedException = new ObjectDisposedException($"Object is disposing (TcpSocket for endpoint: {Endpoint})");
                        SetExceptionToAllPendingTasks(disposedException);
                        _configuration.OnDisconnected?.Invoke(Endpoint, disposedException);
                        return;
                    }

                    var netStream = await netStreamTask.ConfigureAwait(false);
                    await ProcessNetworkstreamTasks(netStream).ConfigureAwait(false);
                } catch (Exception ex) {
                    SetExceptionToAllPendingTasks(ex);
                    _configuration.OnDisconnected?.Invoke(Endpoint, ex);
                }
            }
        }

        private void SetExceptionToAllPendingTasks(Exception ex)
        {
            if (_sendTaskQueue.Count > 0) {
                _log.ErrorFormat(ex, "TcpSocket received an exception, cancelling all pending tasks");
            }

            var wrappedException = WrappedException(ex);
            _sendTaskQueue.DrainAndApply(t => t.Tcp.TrySetException(wrappedException));
            _readTaskQueue.DrainAndApply(t => t.Tcp.TrySetException(wrappedException));
        }

        private async Task ProcessNetworkstreamTasks(NetworkStream netStream)
        {
            //reading/writing from network steam is not thread safe
            //Read and write operations can be performed simultaneously on an instance of the NetworkStream class without the need for synchronization.
            //As long as there is one unique thread for the write operations and one unique thread for the read operations, there will be no cross-interference
            //between read and write threads and no synchronization is required.
            //https://msdn.microsoft.com/en-us/library/z2xae4f4.aspx

            //Exception need to thrown immediately and not depend on the next task
            var receiveTask = ProcessNetworkstreamTask(netStream, _readTaskQueue, ProcessReceiveTaskAsync);
            var sendTask = ProcessNetworkstreamTask(netStream, _sendTaskQueue, ProcessSentTasksAsync);
            await Task.WhenAny(receiveTask, sendTask).ConfigureAwait(false);
            if (_disposeToken.IsCancellationRequested) return;

            await ThrowTaskExceptionIfFaulted(receiveTask);
            await ThrowTaskExceptionIfFaulted(sendTask);
        }

        private async Task ProcessNetworkstreamTask<T>(Stream stream, AsyncCollection<T> queue, Func<Stream, T, Task> asyncProcess)
        {
            Task lastTask = Task.FromResult(true);
            while (_disposeToken.IsCancellationRequested == false && stream != null) {
                await lastTask;
                var hasAvailableData = await queue.OnHasDataAvailablebool(_disposeToken.Token);
                if (!hasAvailableData) return;

                lastTask = asyncProcess(stream, queue.Pop());
            }
        }

        private async Task ThrowTaskExceptionIfFaulted(Task task)
        {
            if (task.IsFaulted || task.IsCanceled) await task;
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

                        _log.DebugFormat("Receiving data from {0}, desired size {1}", Endpoint, readSize);
                        _configuration.OnReadingChunk?.Invoke(Endpoint, receiveTask.ReadSize, totalBytesReceived, timer.Elapsed);
                        var bytesReceived = await stream.ReadAsync(buffer, totalBytesReceived, readSize, receiveTask.CancellationToken).ConfigureAwait(false);
                        _configuration.OnReadChunk?.Invoke(Endpoint, receiveTask.ReadSize, receiveTask.ReadSize - totalBytesReceived, bytesReceived, timer.Elapsed);
                        _log.DebugFormat("Received data from {0}, actual size {1}", Endpoint, bytesReceived);
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

                    receiveTask.Tcp.TrySetResult(buffer);
                } catch (Exception ex) {
                    timer.Stop();
                    _configuration.OnReadFailed?.Invoke(Endpoint, receiveTask.ReadSize, timer.Elapsed, ex);

                    if (_disposeToken.IsCancellationRequested) {
                        var exception = new ObjectDisposedException($"Object is disposing (TcpSocket for endpoint: {Endpoint})");
                        receiveTask.Tcp.TrySetException(exception);
                        throw exception;
                    }

                    if (ex is ConnectionException) {
                        receiveTask.Tcp.TrySetException(ex);
                        if (_disposeToken.IsCancellationRequested) return;
                        throw;
                    }

                    // if an exception made us lose a connection throw disconnected exception
                    if (_client == null || _client.Connected == false) {
                        var exception = new ConnectionException(Endpoint);
                        receiveTask.Tcp.TrySetException(exception);
                        throw exception;
                    }

                    receiveTask.Tcp.TrySetException(ex);
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
                    _log.DebugFormat("Sending data to {0} with CorrelationId {1}", Endpoint, sendTask.Payload.CorrelationId);
                    _configuration.OnWriting?.Invoke(Endpoint, sendTask.Payload);
                    timer.Start();
                    await stream.WriteAsync(sendTask.Payload.Buffer, 0, sendTask.Payload.Buffer.Length, _disposeToken.Token).ConfigureAwait(false);
                    timer.Stop();
                    _configuration.OnWritten?.Invoke(Endpoint, sendTask.Payload, timer.Elapsed);
                    _log.DebugFormat("Sent data to {0} with CorrelationId {1}", Endpoint, sendTask.Payload.CorrelationId);
                    sendTask.Tcp.TrySetResult(sendTask.Payload);
                } catch (Exception ex) {
                    var wrappedException = WrappedException(ex);
                    _configuration.OnWriteFailed?.Invoke(Endpoint, sendTask.Payload, timer.Elapsed, wrappedException);
                    sendTask.Tcp.TrySetException(wrappedException);
                    throw;
                }
            }
        }

        private Exception WrappedException(Exception ex)
        {
            if (_disposeToken.IsCancellationRequested) {
                return new ObjectDisposedException($"Object is disposing (TcpSocket for endpoint: {Endpoint})");
            }
            return new ConnectionException($"Lost connection to server: {Endpoint}", ex) { Endpoint = Endpoint };
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
            _log.DebugFormat("No connection to {0}: Attempting to connect...", Endpoint);

            return _configuration.ConnectionRetry.AttemptAsync(
                async (attempt, timer) => {
                    _configuration.OnConnecting?.Invoke(Endpoint, attempt, timer.Elapsed);
                    _client = new TcpClient();

                    var connectTask = _client.ConnectAsync(Endpoint.IP.Address, Endpoint.IP.Port);
                    await Task.WhenAny(connectTask, _disposeTask).ConfigureAwait(false);
                    if (_disposeToken.IsCancellationRequested) throw new ObjectDisposedException($"Object is disposing (TcpSocket for endpoint {Endpoint})");

                    await connectTask.ConfigureAwait(false);
                    if (_client.Connected) {
                        _log.DebugFormat("Connection established to {0}", Endpoint);
                        _configuration.OnConnected?.Invoke(Endpoint, attempt, timer.Elapsed);
                        return _client;
                    }
                    throw new ConnectionException(Endpoint);
                },
                (ex, attempt, retry) => {
                    if (retry.HasValue) {
                        _log.WarnFormat(ex, "Failed connection to {0}: Will retry in {1}", Endpoint, retry.Value);
                    } else {
                        _log.WarnFormat(ex, "Failed connection to {0} on attempt {1}", Endpoint, attempt);
                    }
                },
                _disposeToken.Token);
        }

        public void Dispose()
        {
            if (Interlocked.Increment(ref _disposeCount) != 1) return;
            _disposeToken?.Cancel();

            using (_disposeToken) {
                using (_disposeRegistration) {
                    using (_client) { }
                }
            }
        }
    }
}