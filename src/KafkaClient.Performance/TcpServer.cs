using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;

namespace KafkaClient.Performance
{
    public class TcpServer
    {
        public Action<ArraySegment<byte>> OnBytesRead { get; set; }

        private readonly TcpListener _listener;
        private readonly CancellationTokenSource _disposeToken = new CancellationTokenSource();
        private readonly Task _connectTask = null;

        public TcpServer(int port)
        {
            _listener = new TcpListener(IPAddress.Any, port);
            _listener.Start();

            _connectTask = Task.Run(ClientConnectAsync, _disposeToken.Token);
        }

        public async Task WriteBytesAsync(ArraySegment<byte> data)
        {
            await _client.GetStream().WriteAsync(data.Array, data.Offset, data.Count).ConfigureAwait(false);
        }

        private TcpClient _client;

        private async Task ClientConnectAsync()
        {
            var buffer = new byte[4096];
            while (!_disposeToken.IsCancellationRequested) {
                try {
                    using (_client = await _listener.AcceptTcpClientAsync().ConfigureAwait(false)) {
                        var stream = _client.GetStream();

                        while (!_disposeToken.IsCancellationRequested) {
                            var read = await stream.ReadAsync(buffer, 0, buffer.Length, _disposeToken.Token)
                                                   .ThrowIfCancellationRequested(_disposeToken.Token)
                                                   .ConfigureAwait(false);
                            if (read > 0) {
                                OnBytesRead?.Invoke(new ArraySegment<byte>(buffer, 0, read));
                            }
                        }
                    }
                } catch (Exception) {
                    // ignore
                }
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