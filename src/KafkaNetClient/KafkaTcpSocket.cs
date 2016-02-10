using KafkaNet.Common;
using KafkaNet.Model;
using KafkaNet.Protocol;
using KafkaNet.Statistics;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaNet
{
    /// <summary>
    /// The TcpSocket provides an abstraction from the main driver from having to handle connection to and reconnections with a server.
    /// The interface is intentionally limited to only read/write.  All connection and reconnect details are handled internally.
    /// </summary>

    public class KafkaTcpSocket : IKafkaTcpSocket
    {
        public event Action OnServerDisconnected;
        public event Action<int> OnReconnectionAttempt;
        public event Action<int> OnReadFromSocketAttempt;
        public event Action<int> OnBytesReceived;
        public event Action<KafkaDataPayload> OnWriteToSocketAttempt;

        private const int DefaultReconnectionTimeout = 100;
        private const int DefaultReconnectionTimeoutMultiplier = 2;
        private const int MaxReconnectionTimeoutMinutes = 5;

        private readonly CancellationTokenSource _disposeToken = new CancellationTokenSource();
        private readonly CancellationTokenRegistration _disposeRegistration;
        private readonly IKafkaLog _log;
        private readonly KafkaEndpoint _endpoint;
        private readonly TimeSpan _maximumReconnectionTimeout;
        private readonly Task _disposeTask;
        private readonly AsyncCollection<SocketPayloadSendTask> _sendTaskQueue;
        private readonly AsyncCollection<SocketPayloadReadTask> _readTaskQueue;
        private readonly StatisticsTrackerOptions _statisticsTrackerOptions;
        private readonly Task _socketTask;
        private readonly AsyncLock _clientLock = new AsyncLock();
        private TcpClient _client;
        private int _disposeCount;
        private readonly int _maxRetry;

        /// <summary>
        /// Construct socket and open connection to a specified server.
        /// </summary>
        /// <param name="log">Logging facility for verbose messaging of actions.</param>
        /// <param name="endpoint">The IP endpoint to connect to.</param>
        /// <param name="maximumReconnectionTimeout">The maximum time to wait when backing off on reconnection attempts.</param>
        public KafkaTcpSocket(IKafkaLog log, KafkaEndpoint endpoint, int maxRetry, TimeSpan? maximumReconnectionTimeout = null, StatisticsTrackerOptions statisticsTrackerOptions = null)
        {
            _log = log;
            _endpoint = endpoint;
            _maximumReconnectionTimeout = maximumReconnectionTimeout ?? TimeSpan.FromMinutes(MaxReconnectionTimeoutMinutes);
            _maxRetry = maxRetry;
            _statisticsTrackerOptions = statisticsTrackerOptions;
            _sendTaskQueue = new AsyncCollection<SocketPayloadSendTask>();
            _readTaskQueue = new AsyncCollection<SocketPayloadReadTask>();

            //dedicate a long running task to the read/write operations
            _socketTask = Task.Run(async () => { await DedicatedSocketTask(); });

            _disposeTask = _disposeToken.Token.CreateTask();
            _disposeRegistration = _disposeToken.Token.Register(() =>
            {
                _sendTaskQueue.CompleteAdding();
                _readTaskQueue.CompleteAdding();
            });
        }

        #region Interface Implementation...

        /// <summary>
        /// The IP Endpoint to the server.
        /// </summary>
        public KafkaEndpoint Endpoint { get { return _endpoint; } }

        /// <summary>
        /// Read a certain byte array size return only when all bytes received.
        /// </summary>
        /// <param name="readSize">The size in bytes to receive from server.</param>
        /// <returns>Returns a byte[] array with the size of readSize.</returns>
        public Task<byte[]> ReadAsync(int readSize)
        {
            return EnqueueReadTask(readSize, CancellationToken.None);
        }

        /// <summary>
        /// Read a certain byte array size return only when all bytes received.
        /// </summary>
        /// <param name="readSize">The size in bytes to receive from server.</param>
        /// <param name="cancellationToken">A cancellation token which will cancel the request.</param>
        /// <returns>Returns a byte[] array with the size of readSize.</returns>
        public Task<byte[]> ReadAsync(int readSize, CancellationToken cancellationToken)
        {
            return EnqueueReadTask(readSize, cancellationToken);
        }

        /// <summary>
        /// Convenience function to write full buffer data to the server.
        /// </summary>
        /// <param name="payload">The buffer data to send.</param>
        /// <returns>Returns Task handle to the write operation with size of written bytes..</returns>
        public Task<KafkaDataPayload> WriteAsync(KafkaDataPayload payload)
        {
            return WriteAsync(payload, CancellationToken.None);
        }

        /// <summary>
        /// Write the buffer data to the server.
        /// </summary>
        /// <param name="payload">The buffer data to send.</param>
        /// <param name="cancellationToken">A cancellation token which will cancel the request.</param>
        /// <returns>Returns Task handle to the write operation with size of written bytes..</returns>
        public Task<KafkaDataPayload> WriteAsync(KafkaDataPayload payload, CancellationToken cancellationToken)
        {
            return EnqueueWriteTask(payload, cancellationToken);
        }

        #endregion Interface Implementation...

        private Task<KafkaDataPayload> EnqueueWriteTask(KafkaDataPayload payload, CancellationToken cancellationToken)
        {
            var sendTask = new SocketPayloadSendTask(payload, cancellationToken);
            _sendTaskQueue.Add(sendTask);
            if (UseStatisticsTracker())
            {
                StatisticsTracker.QueueNetworkWrite(_endpoint, payload);
            }
            return sendTask.Tcp.Task;
        }

        private Task<byte[]> EnqueueReadTask(int readSize, CancellationToken cancellationToken)
        {
            var readTask = new SocketPayloadReadTask(readSize, cancellationToken);
            _readTaskQueue.Add(readTask);
            return readTask.Tcp.Task;
        }

        /// <summary>
        /// Stop all pendding task when can not establish connection in max retry,
        /// but keep trying to recove and connect to this connection.
        /// Only the broker router can dispose of it.
        /// </summary>
        /// <returns></returns>
        private async Task DedicatedSocketTask()
        {
            while (_disposeToken.IsCancellationRequested == false)
            {
                //block here until we can get connections then start loop pushing data through network stream
                try
                {
                    var netStreamTask = GetStreamAsync();
                    await Task.WhenAny(_disposeTask, netStreamTask).ConfigureAwait(false);

                    if (_disposeToken.IsCancellationRequested)
                    {
                        SetExceptionToAllPendingTasks(new ObjectDisposedException(string.Format("Object is disposing (KafkaTcpSocket for endpoint: {0})", Endpoint)));
                        RaiseServerDisconnectedEvent();
                        return;
                    }

                    var netStream = await netStreamTask.ConfigureAwait(false);
                    await ProcessNetworkstreamTasks(netStream).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    SetExceptionToAllPendingTasks(ex);
                    RaiseServerDisconnectedEvent();
                }
            }
        }
        private void RaiseServerDisconnectedEvent()
        {
            if (OnServerDisconnected != null) OnServerDisconnected();
        }

        private void SetExceptionToAllPendingTasks(Exception ex)
        {
            if (_sendTaskQueue.Count > 0)
                _log.ErrorFormat("KafkaTcpSocket received an exception, cancelling all pending tasks: {0}", ex);

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
            var readTask = ProcessNetworkstreamsSendTask(netStream);
            var sendTask = ProcessNetworkstreamTasksReadTask(netStream);
            await Task.WhenAny(readTask, sendTask);
            if (_disposeToken.IsCancellationRequested) return;
            await ThrowTaskExceptionIfFaulted(readTask);
            await ThrowTaskExceptionIfFaulted(sendTask);
        }

        private async Task ProcessNetworkstreamsSendTask(NetworkStream netStream)
        {
            Task lastSendTask = Task.FromResult(true);
            while (_disposeToken.IsCancellationRequested == false && netStream != null)
            {
                await lastSendTask;
                bool hasAvailableData = await _sendTaskQueue.OnHasDataAvailablebool(_disposeToken.Token);
                if (!hasAvailableData) return;
                var send = _sendTaskQueue.Pop();
                lastSendTask = ProcessSentTasksAsync(netStream, send);
            }
        }

        private async Task ProcessNetworkstreamTasksReadTask(NetworkStream netStream)
        {
            Task lastReadTask = Task.FromResult(true);
            while (_disposeToken.IsCancellationRequested == false && netStream != null)
            {
                await lastReadTask;
                bool hasAvailableData = await _readTaskQueue.OnHasDataAvailablebool(_disposeToken.Token);
                if (!hasAvailableData) return;
                var read = _readTaskQueue.Pop();
                lastReadTask = ProcessReadTaskAsync(netStream, read);
            }
        }

        private async Task ThrowTaskExceptionIfFaulted(Task task)
        {
            if (task.IsFaulted || task.IsCanceled) await task;
        }

        private async Task ProcessReadTaskAsync(NetworkStream netStream, SocketPayloadReadTask readTask)
        {
            using (readTask)
            {
                try
                {
                    if (UseStatisticsTracker())
                    {
                        StatisticsTracker.IncrementGauge(StatisticGauge.ActiveReadOperation);
                    }
                    var readSize = readTask.ReadSize;
                    var result = new List<byte>(readSize);
                    var bytesReceived = 0;

                    while (bytesReceived < readSize)
                    {
                        readSize = readSize - bytesReceived;
                        var buffer = new byte[readSize];

                        if (OnReadFromSocketAttempt != null) OnReadFromSocketAttempt(readSize);

                        bytesReceived = await netStream.ReadAsync(buffer, 0, readSize, readTask.CancellationToken).ConfigureAwait(false);

                        if (OnBytesReceived != null) OnBytesReceived(bytesReceived);

                        if (bytesReceived <= 0)
                        {
                            using (_client)
                            {
                                _client = null;
                                if (_disposeToken.IsCancellationRequested) { return; }

                                throw new BrokerConnectionException(string.Format("Lost connection to server: {0}", _endpoint), _endpoint);
                            }
                        }

                        result.AddRange(buffer.Take(bytesReceived));
                    }

                    readTask.Tcp.TrySetResult(result.ToArray());
                }
                catch (Exception ex)
                {
                    if (_disposeToken.IsCancellationRequested)
                    {
                        var exception = new ObjectDisposedException(string.Format("Object is disposing (KafkaTcpSocket for endpoint: {0})", Endpoint));
                        readTask.Tcp.TrySetException(exception);
                        throw exception;
                    }

                    if (ex is BrokerConnectionException)
                    {
                        readTask.Tcp.TrySetException(ex);
                        if (_disposeToken.IsCancellationRequested) return;
                        throw;
                    }

                    //if an exception made us lose a connection throw disconnected exception
                    if (_client == null || _client.Connected == false)
                    {
                        var exception = new BrokerConnectionException(string.Format("Lost connection to server: {0}", _endpoint), _endpoint);
                        readTask.Tcp.TrySetException(exception);
                        throw exception;
                    }

                    readTask.Tcp.TrySetException(ex);
                    if (_disposeToken.IsCancellationRequested) return;

                    throw;
                }
                finally
                {
                    if (UseStatisticsTracker())
                    {
                        StatisticsTracker.DecrementGauge(StatisticGauge.ActiveReadOperation);
                    }
                }
            }
        }

        private bool UseStatisticsTracker()
        {
            return _statisticsTrackerOptions == null || _statisticsTrackerOptions.Enable;
        }

        private async Task ProcessSentTasksAsync(NetworkStream netStream, SocketPayloadSendTask sendTask)
        {
            if (sendTask == null) return;

            using (sendTask)
            {
                var failed = false;
                var sw = Stopwatch.StartNew();
                try
                {
                    sw.Restart();
                    if (UseStatisticsTracker())
                    {
                        StatisticsTracker.IncrementGauge(StatisticGauge.ActiveWriteOperation);
                    }
                    if (OnWriteToSocketAttempt != null) OnWriteToSocketAttempt(sendTask.Payload);

                    _log.DebugFormat("Sending data for CorrelationId:{0} Connection:{1}", sendTask.Payload.CorrelationId, Endpoint);
                    await netStream.WriteAsync(sendTask.Payload.Buffer, 0, sendTask.Payload.Buffer.Length, _disposeToken.Token).ConfigureAwait(false);
                    _log.DebugFormat("Data sent to CorrelationId:{0} Connection:{1}", sendTask.Payload.CorrelationId, Endpoint);
                    sendTask.Tcp.TrySetResult(sendTask.Payload);
                }
                catch (Exception ex)
                {
                    failed = true;
                    var wrappedException = WrappedException(ex);
                    sendTask.Tcp.TrySetException(wrappedException);
                    throw;
                }
                finally
                {
                    if (UseStatisticsTracker())
                    {
                        StatisticsTracker.DecrementGauge(StatisticGauge.ActiveWriteOperation);
                        StatisticsTracker.CompleteNetworkWrite(sendTask.Payload, sw.ElapsedMilliseconds, failed);
                    }
                }
            }
        }

        private Exception WrappedException(Exception ex)
        {
            Exception wrappedException;
            if (_disposeToken.IsCancellationRequested)
                wrappedException = new ObjectDisposedException(string.Format("Object is disposing (KafkaTcpSocket for endpoint: {0})", Endpoint));
            else
                wrappedException = new BrokerConnectionException(string.Format("Lost connection to server: {0}", _endpoint), _endpoint, ex);

            return wrappedException;
        }

        private async Task<NetworkStream> GetStreamAsync()
        {
            //using a semaphore here to allow async waiting rather than blocking locks
            using (await _clientLock.LockAsync(_disposeToken.Token).ConfigureAwait(false))
            {
                if ((_client == null || _client.Connected == false) && !_disposeToken.IsCancellationRequested)
                {
                    _client = await ReEstablishConnectionAsync().ConfigureAwait(false);
                }

                return _client == null ? null : _client.GetStream();
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
            _log.DebugFormat("No connection to:{0}.  Attempting to connect...", _endpoint);

            _client = null;

            while (_disposeToken.IsCancellationRequested == false && _maxRetry > attempts)
            {
                attempts++;
                try
                {
                    if (OnReconnectionAttempt != null) OnReconnectionAttempt(attempts);
                    _client = new TcpClient();

                    var connectTask = _client.ConnectAsync(_endpoint.Endpoint.Address, _endpoint.Endpoint.Port);
                    await Task.WhenAny(connectTask, _disposeTask).ConfigureAwait(false);

                    if (_disposeToken.IsCancellationRequested)
                        throw new ObjectDisposedException(string.Format("Object is disposing (KafkaTcpSocket for endpoint: {0})", Endpoint));

                    await connectTask;
                    if (!_client.Connected) throw new BrokerConnectionException(string.Format("Lost connection to server: {0}", _endpoint), _endpoint);
                    _log.DebugFormat("Connection established to:{0}.", _endpoint);

                    return _client;
                }
                catch (Exception ex)
                {
                    reconnectionDelay = reconnectionDelay * DefaultReconnectionTimeoutMultiplier;
                    reconnectionDelay = Math.Min(reconnectionDelay, (int)_maximumReconnectionTimeout.TotalMilliseconds);

                    _log.WarnFormat("Failed connection to:{0}.  Will retry in:{1} Exception{2}", _endpoint, reconnectionDelay, ex.Message);
                    if (_maxRetry < attempts)
                    {
                        throw;
                    }
                }

                await Task.Delay(TimeSpan.FromMilliseconds(reconnectionDelay), _disposeToken.Token).ConfigureAwait(false);
            }

            return _client;
        }

        public void Dispose()
        {
            if (Interlocked.Increment(ref _disposeCount) != 1) return;
            if (_disposeToken != null) _disposeToken.Cancel();

            using (_disposeToken)
            using (_disposeRegistration)
            using (_client) { }
        }
    }

    internal class SocketPayloadReadTask : IDisposable
    {
        public CancellationToken CancellationToken { get; private set; }
        public TaskCompletionSource<byte[]> Tcp { get; set; }
        public int ReadSize { get; set; }

        private readonly CancellationTokenRegistration _cancellationTokenRegistration;

        public SocketPayloadReadTask(int readSize, CancellationToken cancellationToken)
        {
            CancellationToken = cancellationToken;
            Tcp = new TaskCompletionSource<byte[]>();
            ReadSize = readSize;
            _cancellationTokenRegistration = cancellationToken.Register(() => Tcp.TrySetCanceled());
        }

        public void Dispose()
        {
            using (_cancellationTokenRegistration)
            {
            }
        }
    }

    internal class SocketPayloadSendTask : IDisposable
    {
        public TaskCompletionSource<KafkaDataPayload> Tcp { get; set; }
        public KafkaDataPayload Payload { get; set; }

        private readonly CancellationTokenRegistration _cancellationTokenRegistration;

        public SocketPayloadSendTask(KafkaDataPayload payload, CancellationToken cancellationToken)
        {
            Tcp = new TaskCompletionSource<KafkaDataPayload>();
            Payload = payload;
            _cancellationTokenRegistration = cancellationToken.Register(() => { Tcp.TrySetCanceled(); });
        }

        public void Dispose()
        {
            using (_cancellationTokenRegistration)
            {
            }
        }
    }

    public class KafkaDataPayload
    {
        public int CorrelationId { get; set; }
        public ApiKeyRequestType ApiKey { get; set; }
        public int MessageCount { get; set; }

        public bool TrackPayload
        {
            get { return MessageCount > 0; }
        }

        public byte[] Buffer { get; set; }
    }
}