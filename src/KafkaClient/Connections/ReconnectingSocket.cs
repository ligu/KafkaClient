using System;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;

namespace KafkaClient.Connections
{
    internal class ReconnectingSocket : IDisposable
    {
        private readonly Endpoint _endpoint;
        private readonly ILog _log;
        private readonly bool _isBlocking;
        private readonly IConnectionConfiguration _configuration;

        private int _disposeCount; // = 0;
        private readonly CancellationTokenSource _disposeToken = new CancellationTokenSource();

        private Socket _socket;
        private readonly SemaphoreSlim _connectSemaphore = new SemaphoreSlim(1, 1);

        public ReconnectingSocket(Endpoint endpoint, IConnectionConfiguration configuration, ILog log, bool isBlocking)
        {
            if (endpoint == null) throw new ArgumentNullException(nameof(endpoint));

            _configuration = configuration;
            _endpoint = endpoint;
            _log = log;
            _isBlocking = isBlocking;
        }

        private Socket CreateSocket()
        {
            if (_disposeCount > 0) throw new ObjectDisposedException($"Connection to {_endpoint}");

            var socket = new Socket(_endpoint.Ip.AddressFamily, SocketType.Stream, ProtocolType.Tcp) {
                Blocking = _isBlocking,
                SendTimeout = (int)_configuration.RequestTimeout.TotalMilliseconds,
                SendBufferSize = _configuration.WriteBufferSize,
                ReceiveBufferSize = _configuration.ReadBufferSize,
            };

            if (_configuration.IsTcpKeepalive) {
                socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
            }

            return socket;
        }

        public void Disconnect(Socket socket)
        {
            try {
                Interlocked.CompareExchange(ref _socket, socket, null);
                if (socket == null) return;
                _log.Info(() => LogEvent.Create($"Disposing transport to {_endpoint}"));
                using (socket) {
                    if (socket.Connected) {
                        socket.Shutdown(SocketShutdown.Both);
                    }
                }
            } catch (Exception ex) {
                _log.Info(() => LogEvent.Create(ex));
            }
        }

        public async Task<Socket> ConnectAsync(CancellationToken cancellationToken)
        {
            if (_disposeCount > 0) throw new ObjectDisposedException(nameof(ReconnectingSocket));
            if (_socket?.Connected ?? cancellationToken.IsCancellationRequested) return _socket;

            using (var cancellation = CancellationTokenSource.CreateLinkedTokenSource(_disposeToken.Token, cancellationToken)) {
                return await _connectSemaphore.LockAsync(
                    async () => {
                        if (_socket?.Connected ?? cancellation.Token.IsCancellationRequested) return _socket;
                        var socket = _socket;
                        _socket = await _configuration.ConnectionRetry.TryAsync(
                            //action
                            async (retryAttempt, elapsed) => {
                                if (cancellation.Token.IsCancellationRequested) return RetryAttempt<Socket>.Abort;

                                if (socket == null) {
                                    _log.Info(() => LogEvent.Create($"Creating new socket to {_endpoint}"));
                                    socket = CreateSocket();
                                }

                                _log.Info(() => LogEvent.Create($"Connecting to {_endpoint}"));
                                _configuration.OnConnecting?.Invoke(_endpoint, retryAttempt, elapsed);

                                await socket.ConnectAsync(_endpoint.Ip.Address, _endpoint.Ip.Port).ThrowIfCancellationRequested(cancellation.Token).ConfigureAwait(false);
                                if (!socket.Connected) return RetryAttempt<Socket>.Retry;

                                _log.Info(() => LogEvent.Create($"Connection established to {_endpoint}"));
                                _configuration.OnConnected?.Invoke(_endpoint, retryAttempt, elapsed);
                                return new RetryAttempt<Socket>(socket);
                            },
                            (ex, retryAttempt, retryDelay) => {
                                if (_disposeCount > 0) throw new ObjectDisposedException(nameof(ReconnectingSocket), ex);
                                _log.Warn(() => LogEvent.Create(ex, $"Failed connection to {_endpoint} on retry {retryAttempt}: Will retry in {retryDelay}"));

                                if (ex is ObjectDisposedException || ex is PlatformNotSupportedException) {
                                    Disconnect(socket);
                                    socket = null;
                                }
                            },
                            () => {
                                Disconnect(socket);
                                throw new ConnectionException(_endpoint);
                            },
                            cancellation.Token).ConfigureAwait(false);
                        return _socket;
                    }, cancellation.Token).ConfigureAwait(false);
            }
        }

        public void Dispose()
        {
            if (Interlocked.Increment(ref _disposeCount) != 1) return;

            _disposeToken.Cancel();
            _connectSemaphore.Dispose();
            Disconnect(_socket);
        }
    }
}