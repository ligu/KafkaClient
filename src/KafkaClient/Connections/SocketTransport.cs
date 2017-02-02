using System;
using System.Diagnostics;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;

namespace KafkaClient.Connections
{
    public class SocketTransport : ITransport
    {
        private readonly ReconnectingSocket _socket;
        private Socket _tcpSocket;

        private readonly Endpoint _endpoint;
        private readonly IConnectionConfiguration _configuration;
        private readonly ILog _log;

        private int _disposeCount; // = 0;
        private readonly SemaphoreSlim _writeSemaphore = new SemaphoreSlim(1, 1);

        public SocketTransport(Endpoint endpoint, IConnectionConfiguration configuration, ILog log)
        {
            if (configuration?.SslConfiguration != null) throw new ArgumentOutOfRangeException(nameof(configuration), "Does not support SslConfiguration");

            _endpoint = endpoint;
            _configuration = configuration;
            _log = log;
            _socket = new ReconnectingSocket(endpoint, configuration, log, false);
        }

        public async Task ConnectAsync(CancellationToken cancellationToken)
        {
            _tcpSocket = await _socket.ConnectAsync(cancellationToken);
        }

        public async Task<int> ReadBytesAsync(byte[] buffer, int bytesToRead, Action<int> onBytesRead, CancellationToken cancellationToken)
        {
            var timer = new Stopwatch();
            var totalBytesRead = 0;
            try {
                _configuration.OnReading?.Invoke(_endpoint, bytesToRead);
                timer.Start();
                while (totalBytesRead < bytesToRead && !cancellationToken.IsCancellationRequested) {
                    var bytesRemaining = bytesToRead - totalBytesRead;
                    _log.Verbose(() => LogEvent.Create($"Reading ({bytesRemaining}? bytes) from {_endpoint}"));
                    _configuration.OnReadingBytes?.Invoke(_endpoint, bytesRemaining);
                    var bytes = new ArraySegment<byte>(buffer, 0, Math.Min(buffer.Length, bytesRemaining));
                    var bytesRead = await _tcpSocket.ReceiveAsync(bytes, SocketFlags.None).ThrowIfCancellationRequested(cancellationToken).ConfigureAwait(false);
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
                if (_disposeCount > 0) throw new ObjectDisposedException(nameof(SocketTransport));
                throw;
            }
            return totalBytesRead;
        }

        public async Task<int> WriteBytesAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken, int correlationId = 0)
        {
            var totalBytesWritten = 0;
            await _writeSemaphore.LockAsync( // serialize sending on a given transport
                async () => {
                    var timer = Stopwatch.StartNew();
                    while (totalBytesWritten < buffer.Count) {
                        cancellationToken.ThrowIfCancellationRequested();

                        var bytesRemaining = buffer.Count - totalBytesWritten;
                        _log.Verbose(() => LogEvent.Create($"Writing {bytesRemaining}? bytes (id {correlationId}) to {_endpoint}"));
                        _configuration.OnWritingBytes?.Invoke(_endpoint, bytesRemaining);
                        var bytesWritten = await _tcpSocket.SendAsync(new ArraySegment<byte>(buffer.Array, buffer.Offset + totalBytesWritten, bytesRemaining), SocketFlags.None).ConfigureAwait(false);
                        _configuration.OnWroteBytes?.Invoke(_endpoint, bytesRemaining, bytesWritten, timer.Elapsed);
                        _log.Verbose(() => LogEvent.Create($"Wrote {bytesWritten} bytes (id {correlationId}) to {_endpoint}"));
                        totalBytesWritten += bytesWritten;
                    }
                }, cancellationToken).ConfigureAwait(false);
            return totalBytesWritten;
        }

        public void Dispose()
        {
            if (Interlocked.Increment(ref _disposeCount) != 1) return;

            _writeSemaphore.Dispose();
            _socket.Dispose();
        }        
    }
}