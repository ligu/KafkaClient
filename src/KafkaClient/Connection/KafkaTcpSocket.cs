using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
    public class KafkaTcpSocket : IKafkaTcpSocket
    {
        public event Action OnServerDisconnected;
        public event Action<int> OnReconnectionAttempt;
        public event Action<int> OnReceivingFromSocket;
        public event Action<int> OnReceivedFromSocket;
        public event Action<KafkaDataPayload> OnSendingToSocket;
        public event Action<KafkaDataPayload> OnSentToSocket;

        private const int DefaultReconnectionTimeout = 100;
        private const int DefaultReconnectionTimeoutMultiplier = 2;
        private const int MaxReconnectionTimeoutMinutes = 5;

        private readonly CancellationTokenSource _disposeToken = new CancellationTokenSource();
        private readonly CancellationTokenRegistration _disposeRegistration;
        private readonly IKafkaLog _log;
        private readonly TimeSpan _connectingTimeout;
        private readonly Task _disposeTask;
        private readonly AsyncCollection<SocketPayloadSendTask> _sendTaskQueue;
        private readonly AsyncCollection<SocketPayloadReceiveTask> _readTaskQueue;
        private readonly bool _trackTelemetry;
        // ReSharper disable NotAccessedField.Local
        private readonly Task _socketTask;
        // ReSharper restore NotAccessedField.Local
        private readonly AsyncLock _clientLock = new AsyncLock();
        private TcpClient _client;
        private int _disposeCount;
        private readonly int _maxRetry;

        /// <summary>
        /// Construct socket and open connection to a specified server.
        /// </summary>
        /// <param name="log">Logging facility for verbose messaging of actions.</param>
        /// <param name="endpoint">The IP endpoint to connect to.</param>
        /// <param name="maxRetry">The maximum number of retries.</param>
        /// <param name="connectingTimeout">The maximum time to wait when backing off on reconnection attempts.</param>
        /// <param name="trackTelemetry">Whether to track telemetry.</param>
        public KafkaTcpSocket(IKafkaLog log, KafkaEndpoint endpoint, int maxRetry, TimeSpan? connectingTimeout = null, bool trackTelemetry = false)
        {
            _log = log;
            Endpoint = endpoint;
            _connectingTimeout = connectingTimeout ?? TimeSpan.FromMinutes(MaxReconnectionTimeoutMinutes);
            _maxRetry = maxRetry;
            _sendTaskQueue = new AsyncCollection<SocketPayloadSendTask>();
            _readTaskQueue = new AsyncCollection<SocketPayloadReceiveTask>();
            _trackTelemetry = trackTelemetry;

            // dedicate a long running task to the read/write operations
            _socketTask = Task.Run(DedicatedSocketTask);

            _disposeTask = _disposeToken.Token.CreateTask();
            _disposeRegistration = _disposeToken.Token.Register(() => {
                _sendTaskQueue.CompleteAdding();
                _readTaskQueue.CompleteAdding();
            });
        }

        #region Interface Implementation...

        /// <summary>
        /// The IP Endpoint to the server.
        /// </summary>
        public KafkaEndpoint Endpoint { get; }

        /// <summary>
        /// Read a certain byte array size return only when all bytes received.
        /// </summary>
        /// <param name="readSize">The size in bytes to receive from server.</param>
        /// <param name="cancellationToken">A cancellation token which will cancel the request.</param>
        /// <returns>Returns a byte[] array with the size of readSize.</returns>
        public Task<byte[]> ReadAsync(int readSize, CancellationToken cancellationToken)
        {
            var readTask = new SocketPayloadReceiveTask(readSize, cancellationToken);
            _readTaskQueue.Add(readTask);
            return readTask.Tcp.Task;
        }

        /// <summary>
        /// Write the buffer data to the server.
        /// </summary>
        /// <param name="payload">The buffer data to send.</param>
        /// <param name="cancellationToken">A cancellation token which will cancel the request.</param>
        /// <returns>Returns Task handle to the write operation with size of written bytes..</returns>
        public Task<KafkaDataPayload> WriteAsync(KafkaDataPayload payload, CancellationToken cancellationToken)
        {
            var sendTask = new SocketPayloadSendTask(payload, cancellationToken);
            _sendTaskQueue.Add(sendTask);
            if (_trackTelemetry) {
                StatisticsTracker.QueueNetworkWrite(Endpoint, payload);
            }
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
                        SetExceptionToAllPendingTasks(new ObjectDisposedException($"Object is disposing (KafkaTcpSocket for endpoint: {Endpoint})"));
                        OnServerDisconnected?.Invoke();
                        return;
                    }

                    var netStream = await netStreamTask.ConfigureAwait(false);
                    await ProcessNetworkstreamTasks(netStream).ConfigureAwait(false);
                } catch (Exception ex) {
                    SetExceptionToAllPendingTasks(ex);
                    OnServerDisconnected?.Invoke();
                }
            }
        }

        private void SetExceptionToAllPendingTasks(Exception ex)
        {
            if (_sendTaskQueue.Count > 0) {
                _log.ErrorFormat(ex, "KafkaTcpSocket received an exception, cancelling all pending tasks");
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
                using (_trackTelemetry ? StatisticsTracker.Gauge(StatisticGauge.ActiveReadOperation) : Disposable.None) {
                    try {
                        var readSize = receiveTask.ReadSize;
                        var result = new List<byte>();
                        var bytesReceived = 0;

                        while (bytesReceived < readSize) {
                            readSize = readSize - bytesReceived;
                            var buffer = new byte[readSize];

                            _log.DebugFormat("Receiving data from {0}, desired size {1}", Endpoint, readSize);
                            OnReceivingFromSocket?.Invoke(readSize);
                            bytesReceived = await stream.ReadAsync(buffer, 0, readSize, receiveTask.CancellationToken).ConfigureAwait(false);
                            OnReceivedFromSocket?.Invoke(bytesReceived);
                            _log.DebugFormat("Received data from {0}, actual size {1}", Endpoint, bytesReceived);

                            if (bytesReceived <= 0) {
                                using (_client) {
                                    _client = null;
                                    if (_disposeToken.IsCancellationRequested) { return; }

                                    throw new KafkaConnectionException(Endpoint);
                                }
                            }

                            result.AddRange(buffer.Take(bytesReceived));
                        }

                        receiveTask.Tcp.TrySetResult(result.ToArray());
                    } catch (Exception ex) {
                        if (_disposeToken.IsCancellationRequested) {
                            var exception = new ObjectDisposedException($"Object is disposing (KafkaTcpSocket for endpoint: {Endpoint})");
                            receiveTask.Tcp.TrySetException(exception);
                            throw exception;
                        }

                        if (ex is KafkaConnectionException) {
                            receiveTask.Tcp.TrySetException(ex);
                            if (_disposeToken.IsCancellationRequested) return;
                            throw;
                        }

                        // if an exception made us lose a connection throw disconnected exception
                        if (_client == null || _client.Connected == false) {
                            var exception = new KafkaConnectionException(Endpoint);
                            receiveTask.Tcp.TrySetException(exception);
                            throw exception;
                        }

                        receiveTask.Tcp.TrySetException(ex);
                        if (_disposeToken.IsCancellationRequested) return;

                        throw;
                    }
                }
            }
        }

        private async Task ProcessSentTasksAsync(Stream stream, SocketPayloadSendTask sendTask)
        {
            if (sendTask == null) return;

            using (sendTask) {
                using (_trackTelemetry ? StatisticsTracker.TrackNetworkWrite(sendTask) : Disposable.None) {
                    try {
                        _log.DebugFormat("Sending data to {0} with CorrelationId {1}", Endpoint, sendTask.Payload.CorrelationId);
                        OnSendingToSocket?.Invoke(sendTask.Payload);
                        await stream.WriteAsync(sendTask.Payload.Buffer, 0, sendTask.Payload.Buffer.Length, _disposeToken.Token).ConfigureAwait(false);
                        OnSentToSocket?.Invoke(sendTask.Payload);
                        _log.DebugFormat("Sent data to {0} with CorrelationId {1}", Endpoint, sendTask.Payload.CorrelationId);
                        sendTask.Tcp.TrySetResult(sendTask.Payload);
                    } catch (Exception ex) {
                        var wrappedException = WrappedException(ex);
                        sendTask.Tcp.TrySetException(wrappedException);
                        throw;
                    }
                }
            }
        }

        private Exception WrappedException(Exception ex)
        {
            if (_disposeToken.IsCancellationRequested) {
                return new ObjectDisposedException($"Object is disposing (KafkaTcpSocket for endpoint: {Endpoint})");
            }
            return new KafkaConnectionException($"Lost connection to server: {Endpoint}", ex) { Endpoint = Endpoint };
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
        private async Task<TcpClient> ReEstablishConnectionAsync()
        {
            var attempts = 1;
            var reconnectionDelay = DefaultReconnectionTimeout;
            _log.DebugFormat("No connection to {0}: Attempting to connect...", Endpoint);

            _client = null;
            while (_disposeToken.IsCancellationRequested == false && _maxRetry > attempts) {
                attempts++;
                try {
                    OnReconnectionAttempt?.Invoke(attempts);
                    _client = new TcpClient();

                    var connectTask = _client.ConnectAsync(Endpoint.Endpoint.Address, Endpoint.Endpoint.Port);
                    await Task.WhenAny(connectTask, _disposeTask).ConfigureAwait(false);

                    if (_disposeToken.IsCancellationRequested) throw new ObjectDisposedException($"Object is disposing (KafkaTcpSocket for endpoint {Endpoint})");

                    await connectTask;
                    if (!_client.Connected) throw new KafkaConnectionException(Endpoint);

                    _log.DebugFormat("Connection established to {0}", Endpoint);
                    return _client;
                } catch (Exception ex) {
                    if (_maxRetry < attempts) {
                        _log.WarnFormat(ex, "Failed connection to {0} on retry {1}", Endpoint, attempts);
                        throw;
                    }

                    reconnectionDelay = reconnectionDelay * DefaultReconnectionTimeoutMultiplier;
                    reconnectionDelay = Math.Min(reconnectionDelay, (int)_connectingTimeout.TotalMilliseconds);
                    _log.WarnFormat(ex, "Failed connection to {0}: Will retry in {1}", Endpoint, reconnectionDelay);
                }

                await Task.Delay(TimeSpan.FromMilliseconds(reconnectionDelay), _disposeToken.Token).ConfigureAwait(false);
            }

            return _client;
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