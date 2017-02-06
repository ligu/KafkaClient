using System;
using System.Diagnostics;
using System.IO;
using System.Net.Security;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;

namespace KafkaClient.Connections
{
    public class SslTransport : ITransport
    {
        private Socket _tcpSocket;
        private Stream _stream;

        private readonly Endpoint _endpoint;
        private readonly IConnectionConfiguration _configuration;
        private readonly ISslConfiguration _sslConfiguration;
        private readonly ILog _log;

        private int _disposeCount; // = 0;
        private readonly CancellationTokenSource _disposeToken = new CancellationTokenSource();

        private readonly SemaphoreSlim _connectSemaphore = new SemaphoreSlim(1, 1);
        private readonly SemaphoreSlim _writeSemaphore = new SemaphoreSlim(1, 1);
        private readonly SemaphoreSlim _readSemaphore = new SemaphoreSlim(1, 1);

        public SslTransport(Endpoint endpoint, IConnectionConfiguration configuration, ILog log)
        {
            if (configuration?.SslConfiguration == null) throw new ArgumentOutOfRangeException(nameof(configuration), "Must have SslConfiguration set");
            _sslConfiguration = configuration.SslConfiguration;

            _endpoint = endpoint;
            _configuration = configuration;
            _log = log;
        }

        private Socket CreateSocket()
        {
            if (_endpoint.Ip == null) throw new ConnectionException(_endpoint);
            if (_disposeCount > 0) throw new ObjectDisposedException($"Connection to {_endpoint}");

            var socket = new Socket(_endpoint.Ip.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
            {
                Blocking = true,
                SendTimeout = (int)_configuration.RequestTimeout.TotalMilliseconds,
                SendBufferSize = _configuration.WriteBufferSize,
                ReceiveBufferSize = _configuration.ReadBufferSize,
            };

            if (_configuration.IsTcpKeepalive)
            {
                socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
            }

            return socket;
        }

        public async Task ConnectAsync(CancellationToken cancellationToken)
        {
            if (_disposeCount > 0) throw new ObjectDisposedException(nameof(SslTransport));
            if (_tcpSocket?.Connected ?? cancellationToken.IsCancellationRequested) return;

            using (var cancellation = CancellationTokenSource.CreateLinkedTokenSource(_disposeToken.Token, cancellationToken))
            {
                await _connectSemaphore.LockAsync(
                    async () => {
                        if (_tcpSocket?.Connected ?? cancellation.Token.IsCancellationRequested) return;
                        var socket = _tcpSocket ?? CreateSocket();
                        _tcpSocket = await _configuration.ConnectionRetry.TryAsync(
                            //action
                            async (attempt, elapsed) => {
                                if (cancellation.Token.IsCancellationRequested) return RetryAttempt<Socket>.Abort;

                                _log.Info(() => LogEvent.Create($"Connecting to {_endpoint}"));
                                _configuration.OnConnecting?.Invoke(_endpoint, attempt, elapsed);

                                await socket.ConnectAsync(_endpoint.Ip.Address, _endpoint.Ip.Port).ThrowIfCancellationRequested(cancellation.Token).ConfigureAwait(false);
                                if (!socket.Connected) return RetryAttempt<Socket>.Retry;

                                _log.Info(() => LogEvent.Create($"Connection established to {_endpoint}"));
                                _configuration.OnConnected?.Invoke(_endpoint, attempt, elapsed);

                                _log.Verbose(() => LogEvent.Create($"Attempting SSL connection to {_endpoint.Host}, SslProtocol:{_sslConfiguration.EnabledProtocols}, Policy:{_sslConfiguration.EncryptionPolicy}"));
                                Interlocked.Exchange(ref _stream, null)?.Dispose();
                                try
                                {
                                    var sslStream = new SslStream(
                                        new NetworkStream(socket, true),
                                        false,
                                        _sslConfiguration.RemoteCertificateValidationCallback,
                                        _sslConfiguration.LocalCertificateSelectionCallback,
                                        _sslConfiguration.EncryptionPolicy
                                    );
                                    await sslStream.AuthenticateAsClientAsync(_endpoint.Host, _sslConfiguration.LocalCertificates, _sslConfiguration.EnabledProtocols, _sslConfiguration.CheckCertificateRevocation).ThrowIfCancellationRequested(cancellationToken).ConfigureAwait(false);
                                    _stream = sslStream;
                                    _tcpSocket = socket;
                                    _log.Info(() => LogEvent.Create($"Successful SSL connection to {_endpoint.Host}, SslProtocol:{sslStream.SslProtocol}, KeyExchange:{sslStream.KeyExchangeAlgorithm}.{sslStream.KeyExchangeStrength}, Cipher:{sslStream.CipherAlgorithm}.{sslStream.CipherStrength}, Hash:{sslStream.HashAlgorithm}.{sslStream.HashStrength}, Authenticated:{sslStream.IsAuthenticated}, MutuallyAuthenticated:{sslStream.IsMutuallyAuthenticated}, Encrypted:{sslStream.IsEncrypted}, Signed:{sslStream.IsSigned}"));
                                }
                                catch (Exception ex)
                                {
                                    _log.Warn(() => LogEvent.Create(ex, "SSL connection failed"));
                                    Interlocked.Exchange(ref _stream, null)?.Dispose();
                                }

                                return new RetryAttempt<Socket>(socket);
                            },
                            (ex, attempt, retry) => {
                                if (_disposeCount > 0) throw new ObjectDisposedException(nameof(SslTransport), ex);
                                _log.Warn(() => LogEvent.Create(ex, $"Failed connection to {_endpoint}: Will retry in {retry}"));

                                if (ex is ObjectDisposedException || ex is PlatformNotSupportedException)
                                {
                                    //Disconnect();
                                    _log.Info(() => LogEvent.Create($"Creating new socket to {_endpoint}"));
                                    socket = CreateSocket();
                                }
                            },
                            () => {
                                _log.Warn(() => LogEvent.Create($"Failed connection to {_endpoint}"));
                                throw new ConnectionException(_endpoint);
                            },
                            cancellation.Token).ConfigureAwait(false);
                    }, cancellation.Token).ConfigureAwait(false);
            }
        }

        public async Task<int> ReadBytesAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken)
        {
            var timer = new Stopwatch();
            var totalBytesRead = 0;
            try {
                await _readSemaphore.LockAsync( // serialize receiving on a given transport
                    async () => {
                        _configuration.OnReading?.Invoke(_endpoint, buffer.Count);
                        timer.Start();
                        while (totalBytesRead < buffer.Count && !cancellationToken.IsCancellationRequested) {
                            var bytesRemaining = buffer.Count - totalBytesRead;
                            _log.Verbose(() => LogEvent.Create($"Reading ({bytesRemaining}? bytes) from {_endpoint}"));
                            _configuration.OnReadingBytes?.Invoke(_endpoint, bytesRemaining);
                            var bytesRead = await _stream.ReadAsync(buffer.Array, buffer.Offset + totalBytesRead, bytesRemaining, cancellationToken).ConfigureAwait(false);
                            totalBytesRead += bytesRead;
                            _configuration.OnReadBytes?.Invoke(_endpoint, bytesRemaining, bytesRead, timer.Elapsed);
                            _log.Verbose(() => LogEvent.Create($"Read {bytesRead} bytes from {_endpoint}"));
                        }
                        timer.Stop();
                        _configuration.OnRead?.Invoke(_endpoint, totalBytesRead, timer.Elapsed);
                    }, cancellationToken).ConfigureAwait(false);
            } catch (Exception ex) {
                timer.Stop();
                _configuration.OnReadFailed?.Invoke(_endpoint, buffer.Count, timer.Elapsed, ex);
                if (_disposeCount > 0) throw new ObjectDisposedException(nameof(SslTransport));
                throw;
            }
            return totalBytesRead;
        }

        public async Task<int> WriteBytesAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken, int correlationId = 0)
        {
            var totalBytes = buffer.Count;
            await _writeSemaphore.LockAsync( // serialize sending on a given transport
                async () => {
                    var timer = Stopwatch.StartNew();
                    cancellationToken.ThrowIfCancellationRequested();
                
                    _log.Verbose(() => LogEvent.Create($"Writing {totalBytes}? bytes (id {correlationId}) to {_endpoint}"));
                    _configuration.OnWritingBytes?.Invoke(_endpoint, totalBytes);
                    await _stream.WriteAsync(buffer.Array, buffer.Offset, totalBytes, cancellationToken).ConfigureAwait(false);
                    _configuration.OnWroteBytes?.Invoke(_endpoint, totalBytes, totalBytes, timer.Elapsed);
                    _log.Verbose(() => LogEvent.Create($"Wrote {totalBytes} bytes (id {correlationId}) to {_endpoint}"));
                }, cancellationToken).ConfigureAwait(false);
            return totalBytes;
        }

        public void Dispose()
        {
            if (Interlocked.Increment(ref _disposeCount) > 1) return;

            _writeSemaphore.Dispose();
            _readSemaphore.Dispose();
            _connectSemaphore.Dispose();
            _stream?.Dispose();
        }
    }
}
