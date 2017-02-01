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
    public class StreamTransport : ITransport
    {
        private readonly ILog _log;

        private readonly IConnectionConfiguration _configuration;

        private readonly CancellationTokenSource _disposeToken;

        private readonly Endpoint _endpoint;

        private Socket _socket;
        private Stream _stream;
        private readonly SemaphoreSlim _connectSemaphore = new SemaphoreSlim(1, 1);
        private readonly SemaphoreSlim _sendSemaphore = new SemaphoreSlim(1, 1);

        public StreamTransport(ILog log, IConnectionConfiguration configuration, CancellationTokenSource disposeToken, Endpoint endpoint)
        {
            _log = log;
            _configuration = configuration;
            _disposeToken = disposeToken;
            _endpoint = endpoint;
        }

        private Socket CreateSocket()
        {
            if (_endpoint.Ip == null) throw new ConnectionException(_endpoint);
            if (_disposeToken.IsCancellationRequested) throw new ObjectDisposedException($"Connection to {_endpoint}");

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
            if (_disposeToken.IsCancellationRequested) throw new ObjectDisposedException(nameof(StreamTransport));

            if (_socket?.Connected ?? cancellationToken.IsCancellationRequested) return;

            await _connectSemaphore.LockAsync(
                async () => {
                    if (_socket?.Connected ?? cancellationToken.IsCancellationRequested) return;
                    var socket = _socket ?? CreateSocket();
                    _socket = await _configuration.ConnectionRetry.TryAsync(
                        //action
                        async (attempt, timer) =>
                        {
                            _log.Info(() => LogEvent.Create($"Connecting to {_endpoint}"));
                            _configuration.OnConnecting?.Invoke(_endpoint, attempt, timer.Elapsed);

                            var connectTask = socket.ConnectAsync(_endpoint.Ip.Address, _endpoint.Ip.Port);
                            if (await connectTask.IsCancelled(_disposeToken.Token).ConfigureAwait(false))
                            {
                                throw new ObjectDisposedException($"Object is disposing (TcpSocket for endpoint {_endpoint})");
                            }

                            await connectTask.ConfigureAwait(false);
                            if (!socket.Connected) return RetryAttempt<Socket>.Retry;

                            _log.Info(() => LogEvent.Create($"Connection established to {_endpoint}"));
                            _configuration.OnConnected?.Invoke(_endpoint, attempt, timer.Elapsed);
                            return new RetryAttempt<Socket>(socket);
                        },
                        //onRetry
                        (attempt, retry) => _log.Warn(() => LogEvent.Create($"Failed connection to {_endpoint}: Will retry in {retry}")),
                        //onFinal
                        attempt =>
                        {
                            _log.Warn(() => LogEvent.Create($"Failed connection to {_endpoint} on attempt {attempt}"));
                            throw new ConnectionException(_endpoint);
                        },
                        //onException
                        (ex, attempt, retry) =>
                        {
                            if (_disposeToken.IsCancellationRequested) throw new ObjectDisposedException(nameof(StreamTransport), ex);
                            _log.Warn(() => LogEvent.Create(ex, $"Failed connection to {_endpoint}: Will retry in {retry}"));

                            if (ex is ObjectDisposedException || ex is PlatformNotSupportedException)
                            {
                                DisposeSocket();
                                _log.Info(() => LogEvent.Create($"Creating new socket to {_endpoint}"));
                                socket = CreateSocket();
                            }
                        },
                        //onFinalException
                        (ex, attempt) =>
                        {
                            _log.Warn(() => LogEvent.Create(ex, $"Failed connection to {_endpoint} on attempt {attempt}"));
                            if (ex is SocketException || ex is PlatformNotSupportedException)
                            {
                                throw new ConnectionException(_endpoint, ex);
                            }
                        },
                        cancellationToken).ConfigureAwait(false);
                    //return _socket;
                }, cancellationToken).ConfigureAwait(false);

            var networkStream = new NetworkStream(_socket, false);

            if (_configuration.SslConfiguration == null)
            {
                _stream = networkStream;
                return;
            }

            var sslStream = new SslStream(
                networkStream,
                false,
                _configuration.SslConfiguration.RemoteCertificateValidationCallback,
                _configuration.SslConfiguration.LocalCertificateSelectionCallback,
                _configuration.SslConfiguration.EncryptionPolicy ?? EncryptionPolicy.RequireEncryption
            );

            try
            {
                await sslStream.AuthenticateAsClientAsync(_endpoint.Host).ConfigureAwait(false);
                _log.Info(() => LogEvent.Create($"Successful SSL connection, SslProtocol:{sslStream.SslProtocol}, KeyExchange:{sslStream.KeyExchangeAlgorithm}.{sslStream.KeyExchangeStrength}, Cipher:{sslStream.CipherAlgorithm}.{sslStream.CipherStrength}, Hash:{sslStream.HashAlgorithm}.{sslStream.HashStrength}, Authenticated:{sslStream.IsAuthenticated}, MutuallyAuthenticated:{sslStream.IsMutuallyAuthenticated}, Encrypted:{sslStream.IsEncrypted}, Signed:{sslStream.IsSigned}"));
                _stream = sslStream;
            }
            catch (Exception ex)
            {
                _log.Warn(() => LogEvent.Create(ex, $"SSL connection failed: {ex.Message}"));
                sslStream.Dispose();
            }
            
        }

        public async Task<int> ReadBytesAsync(byte[] buffer, int bytesToRead, Action<int> onBytesRead, CancellationToken cancellationToken)
        {
            var timer = new Stopwatch();
            var totalBytesRead = 0;
            var token = _disposeToken.Token;
            CancellationTokenSource cancellation = null;
            if (cancellationToken.CanBeCanceled)
            {
                cancellation = CancellationTokenSource.CreateLinkedTokenSource(_disposeToken.Token, cancellationToken);
                token = cancellation.Token;
            }
            try
            {
                _configuration.OnReading?.Invoke(_endpoint, bytesToRead);
                timer.Start();
                while (totalBytesRead < bytesToRead && !token.IsCancellationRequested)
                {
                    var bytesRemaining = bytesToRead - totalBytesRead;
                    _log.Verbose(() => LogEvent.Create($"Reading ({bytesRemaining}? bytes) from {_endpoint}"));
                    _configuration.OnReadingBytes?.Invoke(_endpoint, bytesRemaining);
                    var bytesRead = await _stream.ReadAsync(buffer, totalBytesRead, bytesRemaining, token).ConfigureAwait(false);
                    totalBytesRead += bytesRead;
                    _configuration.OnReadBytes?.Invoke(_endpoint, bytesRemaining, bytesRead, timer.Elapsed);
                    _log.Verbose(() => LogEvent.Create($"Read {bytesRead} bytes from {_endpoint}"));

                    if (bytesRead <= 0 && _socket.Available == 0)
                    {
                        DisposeSocket();
                        var ex = new ConnectionException(_endpoint);
                        _configuration.OnDisconnected?.Invoke(_endpoint, ex);
                        throw ex;
                    }
                    onBytesRead(bytesRead);
                }
                timer.Stop();
                _configuration.OnRead?.Invoke(_endpoint, totalBytesRead, timer.Elapsed);
            }
            catch (Exception ex)
            {
                timer.Stop();
                _configuration.OnReadFailed?.Invoke(_endpoint, bytesToRead, timer.Elapsed, ex);
                if (_disposeToken.IsCancellationRequested) throw new ObjectDisposedException(nameof(StreamTransport));
                throw;
            }
            finally
            {
                cancellation?.Dispose();
            }
            return totalBytesRead;
        }

        public async Task<int> WriteBytesAsync(int correlationId, ArraySegment<byte> buffer, CancellationToken cancellationToken)
        {
            await _sendSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

            var bytesRemaining = buffer.Count;
            try
            {
                var timer = Stopwatch.StartNew();
                _disposeToken.Token.ThrowIfCancellationRequested();
                cancellationToken.ThrowIfCancellationRequested();
                
                _log.Verbose(() => LogEvent.Create($"Writing {bytesRemaining}? bytes (id {correlationId}) to {_endpoint}"));
                _configuration.OnWritingBytes?.Invoke(_endpoint, bytesRemaining);
                await _stream.WriteAsync(buffer.Array, buffer.Offset, bytesRemaining, cancellationToken).ConfigureAwait(false);
                _configuration.OnWroteBytes?.Invoke(_endpoint, bytesRemaining, bytesRemaining, timer.Elapsed);
                _log.Verbose(() => LogEvent.Create($"Wrote {bytesRemaining} bytes (id {correlationId}) to {_endpoint}"));
            }
            catch (Exception ex)
            {
                if (_disposeToken.IsCancellationRequested) throw new ObjectDisposedException(nameof(StreamTransport));
                throw;
            }
            finally
            {
                _sendSemaphore.Release(1);
            }
            return bytesRemaining;
        }

        private void DisposeSocket()
        {
            try
            {
                if (_socket == null) return;
                _log.Info(() => LogEvent.Create($"Disposing connection to {_endpoint}"));
                using (_socket)
                {
                    if (_socket.Connected)
                    {
                        _socket.Shutdown(SocketShutdown.Both);
                    }
                }
            }
            catch (Exception ex)
            {
                _log.Info(() => LogEvent.Create(ex));
            }
        }

        public void Dispose()
        {
            DisposeSocket();
            _connectSemaphore.Dispose();
            _sendSemaphore.Dispose();
        }
    }
}
