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
        private readonly ReconnectingSocket _socket;

        private readonly Endpoint _endpoint;
        private readonly IConnectionConfiguration _configuration;
        private readonly ILog _log;

        private int _disposeCount; // = 0;
        private readonly SemaphoreSlim _sendSemaphore = new SemaphoreSlim(1, 1);

        public SslTransport(Endpoint endpoint, IConnectionConfiguration configuration, ILog log)
        {
            if (configuration?.SslConfiguration == null) throw new ArgumentOutOfRangeException(nameof(configuration), "Must have SslConfiguration set");

            _endpoint = endpoint;
            _configuration = configuration;
            _log = log;
            _socket = new ReconnectingSocket(endpoint, configuration, log, true);
        }

        public async Task ConnectAsync(CancellationToken cancellationToken)
        {
            var socket = await _socket.ConnectAsync(cancellationToken);
            if (ReferenceEquals(_tcpSocket, socket)) return;

            NetworkStream networkStream = null;
            SslStream sslStream = null;
            try {
                networkStream = new NetworkStream(socket, false);
                sslStream = new SslStream(
                    networkStream,
                    false,
                    _configuration.SslConfiguration.RemoteCertificateValidationCallback,
                    _configuration.SslConfiguration.LocalCertificateSelectionCallback,
                    _configuration.SslConfiguration.EncryptionPolicy ?? EncryptionPolicy.RequireEncryption
                );
                await sslStream.AuthenticateAsClientAsync(_endpoint.Host).ConfigureAwait(false);
                _stream = sslStream;
                _log.Info(() => LogEvent.Create($"Successful SSL connection, SslProtocol:{sslStream.SslProtocol}, KeyExchange:{sslStream.KeyExchangeAlgorithm}.{sslStream.KeyExchangeStrength}, Cipher:{sslStream.CipherAlgorithm}.{sslStream.CipherStrength}, Hash:{sslStream.HashAlgorithm}.{sslStream.HashStrength}, Authenticated:{sslStream.IsAuthenticated}, MutuallyAuthenticated:{sslStream.IsMutuallyAuthenticated}, Encrypted:{sslStream.IsEncrypted}, Signed:{sslStream.IsSigned}"));
                _tcpSocket = socket;
            } catch (Exception ex) {
                _log.Warn(() => LogEvent.Create(ex, "SSL connection failed"));
                if (sslStream == null) {
                    networkStream?.Dispose();
                } else {
                    sslStream.Dispose();
                }
            }
        }

        public async Task<int> ReadBytesAsync(byte[] buffer, int bytesToRead, Action<int> onBytesRead, CancellationToken cancellationToken)
        {
            var timer = new Stopwatch();
            var totalBytesRead = 0;
            try {
                _configuration.OnReading?.Invoke(_endpoint, bytesToRead);
                timer.Start();
                while (totalBytesRead < bytesToRead && !cancellationToken.IsCancellationRequested)
                {
                    var bytesRemaining = bytesToRead - totalBytesRead;
                    _log.Verbose(() => LogEvent.Create($"Reading ({bytesRemaining}? bytes) from {_endpoint}"));
                    _configuration.OnReadingBytes?.Invoke(_endpoint, bytesRemaining);
                    var bytesRead = await _stream.ReadAsync(buffer, totalBytesRead, bytesRemaining, cancellationToken).ConfigureAwait(false);
                    totalBytesRead += bytesRead;
                    _configuration.OnReadBytes?.Invoke(_endpoint, bytesRemaining, bytesRead, timer.Elapsed);
                    _log.Verbose(() => LogEvent.Create($"Read {bytesRead} bytes from {_endpoint}"));

                    if (bytesRead <= 0 && _socket.Available == 0) {
                        _socket.Disconnect();
                        var ex = new ConnectionException(_endpoint);
                        _configuration.OnDisconnected?.Invoke(_endpoint, ex);
                        throw ex;
                    }
                    onBytesRead(bytesRead);
                }
                timer.Stop();
                _configuration.OnRead?.Invoke(_endpoint, totalBytesRead, timer.Elapsed);
            } catch (Exception ex) {
                timer.Stop();
                _configuration.OnReadFailed?.Invoke(_endpoint, bytesToRead, timer.Elapsed, ex);
                if (_disposeCount > 0) throw new ObjectDisposedException(nameof(SslTransport));
                throw;
            }
            return totalBytesRead;
        }

        public async Task<int> WriteBytesAsync(int correlationId, ArraySegment<byte> buffer, CancellationToken cancellationToken)
        {
            var bytesRemaining = buffer.Count;
            await _sendSemaphore.LockAsync( // serialize sending on a given transport
                async () => {
                    var timer = Stopwatch.StartNew();
                    cancellationToken.ThrowIfCancellationRequested();
                
                    _log.Verbose(() => LogEvent.Create($"Writing {bytesRemaining}? bytes (id {correlationId}) to {_endpoint}"));
                    _configuration.OnWritingBytes?.Invoke(_endpoint, bytesRemaining);
                    await _stream.WriteAsync(buffer.Array, buffer.Offset, bytesRemaining, cancellationToken).ConfigureAwait(false);
                    _configuration.OnWroteBytes?.Invoke(_endpoint, bytesRemaining, bytesRemaining, timer.Elapsed);
                    _log.Verbose(() => LogEvent.Create($"Wrote {bytesRemaining} bytes (id {correlationId}) to {_endpoint}"));
                }, cancellationToken).ConfigureAwait(false);
            return bytesRemaining;
        }

        public void Dispose()
        {
            if (Interlocked.Increment(ref _disposeCount) != 1) return;

            _sendSemaphore.Dispose();
            _stream?.Dispose();
            _socket.Dispose();
        }
    }
}
