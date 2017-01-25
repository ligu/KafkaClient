using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using Nito.AsyncEx;

namespace KafkaClient.Testing
{
    public class TcpServer : IDisposable
    {
        public Action<ArraySegment<byte>> OnBytesReceived { get; set; }
        public Action OnConnected { get; set; }
        public Action OnDisconnected { get; set; }

        private readonly TcpListener _listener;
        private readonly CancellationTokenSource _disposeToken = new CancellationTokenSource();
        private readonly Task _connectTask = null;
        private TaskCompletionSource<bool> _connectedTrigger = new TaskCompletionSource<bool>();

        private readonly ILog _log;

        public TcpServer(int port, ILog log = null)
        {
            _log = log ?? new TraceLog(LogLevel.Error);
            _listener = new TcpListener(IPAddress.Any, port);
            _listener.Start();

            _connectTask = Task.Run(ClientConnectAsync, _disposeToken.Token);
        }

        public async Task<bool> SendDataAsync(ArraySegment<byte> data)
        {
            try {
                await _clientSemaphore.WaitAsync();
                _log.Debug(() => LogEvent.Create($"FAKE Server: writing {data.Count} bytes."));
                await _client.GetStream().WriteAsync(data.Array, data.Offset, data.Count).ConfigureAwait(false);
                return true;
            } catch (Exception ex) {
                _log.Info(() => LogEvent.Create(ex));
                return false;
            } finally {
                _clientSemaphore.Release();
            }
        }

        public void DropConnection()
        {
            Interlocked.Exchange(ref _client, null)?.Dispose();
        }

        private TcpClient _client;
        private readonly AsyncSemaphore _clientSemaphore = new AsyncSemaphore(0);
        public Task ClientConnected => _connectedTrigger.Task;

        private async Task ClientConnectAsync()
        {
            var buffer = new byte[8192];
            while (!_disposeToken.IsCancellationRequested) {
                _log.Debug(() => LogEvent.Create("Server: Accepting clients."));
                _client = await _listener.AcceptTcpClientAsync().ConfigureAwait(false);
                _log.Debug(() => LogEvent.Create("Server: Connected client"));

                _connectedTrigger.TrySetResult(true);
                OnConnected?.Invoke();
                _clientSemaphore.Release();
                try {
                    var stream = _client.GetStream();

                    while (!_disposeToken.IsCancellationRequested) {
                        var read = await stream.ReadAsync(buffer, 0, buffer.Length, _disposeToken.Token)
                                                .ThrowIfCancellationRequested(_disposeToken.Token)
                                                .ConfigureAwait(false);
                        if (read > 0) {
                            OnBytesReceived?.Invoke(new ArraySegment<byte>(buffer, 0, read));
                        }
                    }
                } catch (Exception ex) {
                    if (!(ex is OperationCanceledException)) {
                        _log.Debug(() => LogEvent.Create(ex, "Server: client failed"));
                    }
                } finally {
                    _client?.Dispose();
                }

                _log.Debug(() => LogEvent.Create("Server: Client Disconnected."));
                await _clientSemaphore.WaitAsync(_disposeToken.Token); //remove the one client
                _connectedTrigger = new TaskCompletionSource<bool>();
                OnDisconnected?.Invoke();
            }
        }

        public void Dispose()
        {
            _disposeToken?.Cancel();

            using (_disposeToken) {
                _connectTask?.Wait(TimeSpan.FromSeconds(1));
                _listener.Stop();
            }
        }
    }
}