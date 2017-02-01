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

        private Socket _socket;
        private readonly SemaphoreSlim _connectSemaphore = new SemaphoreSlim(1, 1);

        public ReconnectingSocket(Endpoint endpoint, IConnectionConfiguration configuration, ILog log, bool isBlocking)
        {
            _configuration = configuration;
            _endpoint = endpoint;
            _log = log;
            _isBlocking = isBlocking;
        }

        private Socket CreateSocket()
        {
            if (_endpoint.Ip == null) throw new ConnectionException(_endpoint);
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

        public void Disconnect()
        {
            try {
                var socket = Interlocked.Exchange(ref _socket, null);
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

        public int Available => _socket?.Available ?? 0;

        public async Task<Socket> ConnectAsync(CancellationToken cancellationToken)
        {
            if (_disposeCount > 0) throw new ObjectDisposedException(nameof(ReconnectingSocket));
            if (_socket?.Connected ?? cancellationToken.IsCancellationRequested) return _socket;

            return await _connectSemaphore.LockAsync(
                async () => {
                    if (_socket?.Connected ?? cancellationToken.IsCancellationRequested) return _socket;
                    var socket = _socket ?? CreateSocket();
                    _socket = await _configuration.ConnectionRetry.TryAsync(
                        //action
                        async (attempt, timer) => {
                            if (cancellationToken.IsCancellationRequested) return RetryAttempt<Socket>.Abort;

                            _log.Info(() => LogEvent.Create($"Connecting to {_endpoint}"));
                            _configuration.OnConnecting?.Invoke(_endpoint, attempt, timer.Elapsed);

                            await socket.ConnectAsync(_endpoint.Ip.Address, _endpoint.Ip.Port).ConfigureAwait(false);
                            if (!socket.Connected) return RetryAttempt<Socket>.Retry;

                            _log.Info(() => LogEvent.Create($"Connection established to {_endpoint}"));
                            _configuration.OnConnected?.Invoke(_endpoint, attempt, timer.Elapsed);
                            return new RetryAttempt<Socket>(socket);
                        },
                        (attempt, retry) => _log.Warn(() =>LogEvent.Create($"Failed connection to {_endpoint}: Will retry in {retry}")),
                        attempt => {
                            _log.Warn(() => LogEvent.Create($"Failed connection to {_endpoint} on attempt {attempt}"));
                            throw new ConnectionException(_endpoint);
                        },
                        (ex, attempt, retry) => {
                            if (_disposeCount > 0) throw new ObjectDisposedException(nameof(ReconnectingSocket), ex);
                            _log.Warn(() => LogEvent.Create(ex, $"Failed connection to {_endpoint}: Will retry in {retry}"));

                            if (ex is ObjectDisposedException || ex is PlatformNotSupportedException) {
                                Disconnect();
                                _log.Info(() => LogEvent.Create($"Creating new socket to {_endpoint}"));
                                socket = CreateSocket();
                            }
                        },
                        (ex, attempt) => {
                            _log.Warn(() => LogEvent.Create(ex, $"Failed connection to {_endpoint} on attempt {attempt}"));
                            if (ex is SocketException || ex is PlatformNotSupportedException) {
                                throw new ConnectionException(_endpoint, ex);
                            }
                        },
                        cancellationToken).ConfigureAwait(false);
                    return _socket;
                }, cancellationToken).ConfigureAwait(false);
        }

        public void Dispose()
        {
            if (Interlocked.Increment(ref _disposeCount) != 1) return;

            _connectSemaphore.Dispose();
            Disconnect();
        }
    }
}