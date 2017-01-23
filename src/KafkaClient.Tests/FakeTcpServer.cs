using System;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using Nito.AsyncEx;

namespace KafkaClient.Tests
{
    public class FakeTcpServer : IDisposable
    {
        public Action<byte[]> OnBytesReceived { get; set; }
        public Action OnClientConnected { get; set; }
        public Action OnClientDisconnected { get; set; }

        private readonly ILog _log;

        private TcpClient _client;
        private readonly AsyncSemaphore _semaphoreSlim = new AsyncSemaphore(0);
        private readonly TcpListener _listener;
        private readonly CancellationTokenSource _disposeToken = new CancellationTokenSource();
        private TaskCompletionSource<bool> _clientConnectedTrigger = new TaskCompletionSource<bool>();

        private readonly Task _clientConnectionHandlerTask = null;

        public int ConnectionEventcount = 0;
        public int DisconnectionEventCount = 0;
        public Task HasClientConnected => _clientConnectedTrigger.Task;

        public FakeTcpServer(ILog log, int port)
        {
            _log = log;
            _listener = new TcpListener(IPAddress.Any, port);
            _listener.Start();

            _clientConnectionHandlerTask = Task.Run(StartHandlingClientRequestAsync, _disposeToken.Token);
        }

        public Task SendDataAsync(byte[] data)
        {
            return SendDataAsync(new ArraySegment<byte>(data));
        }

        public async Task SendDataAsync(ArraySegment<byte> data)
        {
            try {
                await _semaphoreSlim.WaitAsync();
                _log.Info(() => LogEvent.Create($"FAKE Server: writing {data.Count} bytes."));
                await _client.GetStream().WriteAsync(data.Array, data.Offset, data.Count).ConfigureAwait(false);
            } catch (Exception ex) {
                _log.Error(LogEvent.Create(ex));
            } finally {
                _semaphoreSlim.Release();
            }
        }

        public Task SendDataAsync(string data)
        {
            var msg = Encoding.ASCII.GetBytes(data);
            return SendDataAsync(new ArraySegment<byte>(msg));
        }

        public void DropConnection()
        {
            if (_client != null) {
                using (_client) {
                }

                _client = null;
            }
        }

        private async Task StartHandlingClientRequestAsync()
        {
            while (!_disposeToken.IsCancellationRequested) {
                _log.Info(() => LogEvent.Create("FAKE Server: Accepting clients."));
                _client = await _listener.AcceptTcpClientAsync();

                _log.Info(() => LogEvent.Create("FAKE Server: Connected client"));
                _clientConnectedTrigger.TrySetResult(true);
                Interlocked.Increment(ref ConnectionEventcount);
                OnClientConnected?.Invoke();
                _semaphoreSlim.Release();

                try {
                    using (_client) {
                        var buffer = new byte[4096];
                        var stream = _client.GetStream();

                        while (!_disposeToken.IsCancellationRequested) {
                            var bytesReceived = await stream.ReadAsync(buffer, 0, buffer.Length, _disposeToken.Token).ThrowIfCancellationRequested(_disposeToken.Token);
                            if (bytesReceived > 0) {
                                OnBytesReceived?.Invoke(buffer.Take(bytesReceived).ToArray());
                            }
                        }
                    }
                } catch (Exception ex) {
                    _log.Error(LogEvent.Create(ex, "FAKE Server: Client exception..."));
                }

                _log.Warn(() => LogEvent.Create("FAKE Server: Client Disconnected."));
                await _semaphoreSlim.WaitAsync(_disposeToken.Token); //remove the one client
                _clientConnectedTrigger = new TaskCompletionSource<bool>();
                Interlocked.Increment(ref DisconnectionEventCount);
                OnClientDisconnected?.Invoke();
            }
        }

        public void Dispose()
        {
            _disposeToken?.Cancel();

            using (_disposeToken) {
                _clientConnectionHandlerTask?.Wait(TimeSpan.FromSeconds(1));

                _listener.Stop();
            }
        }
    }
}