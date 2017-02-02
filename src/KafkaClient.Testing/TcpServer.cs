using System;
using System.IO;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;

namespace KafkaClient.Testing
{
    public class TcpServer : IDisposable
    {
        private readonly X509Certificate _certificate;

        // public hooks
        public Func<ArraySegment<byte>, Task> OnReceivedAsync { get; set; }
        public Action OnConnected { get; set; }
        public Action OnDisconnected { get; set; }
        public Task ClientConnected => _connectedTrigger.Task;

        private readonly CancellationTokenSource _disposeToken = new CancellationTokenSource();
        private readonly Task _connectTask = null;
        private TaskCompletionSource<bool> _connectedTrigger = new TaskCompletionSource<bool>();
        private readonly TcpListener _listener;
        private TcpClient _client;
        private Stream _stream;
        private readonly SemaphoreSlim _clientSemaphore = new SemaphoreSlim(0, 1);

        private readonly ILog _log;

        public TcpServer(int port, ILog log = null, X509Certificate certificate = null)
        {
            _certificate = certificate;
            _log = log ?? new TraceLog(LogLevel.Error);
            _listener = new TcpListener(IPAddress.Loopback, port);
            _listener.Start();

            _connectTask = Task.Run(ClientConnectAsync, _disposeToken.Token);
        }

        public void DropConnection()
        {
            Interlocked.Exchange(ref _stream, null)?.Dispose();
            Interlocked.Exchange(ref _client, null)?.Dispose();
        }

        public async Task<bool> SendDataAsync(ArraySegment<byte> data)
        {
            return await _clientSemaphore.LockAsync(
                async () => {
                    try {
                        _log.Debug(() => LogEvent.Create($"SERVER: writing {data.Count} bytes."));
                        await _stream.WriteAsync(data.Array, data.Offset, data.Count, _disposeToken.Token).ConfigureAwait(false);
                        _log.Verbose(() => LogEvent.Create($"SERVER: wrote {data.Count} bytes."));
                        //await stream.FlushAsync(_disposeToken.Token).ConfigureAwait(false);
                        //_log.Verbose(() => LogEvent.Create($"SERVER: flushed {data.Count} bytes."));
                        return true;
                    } catch (Exception ex) {
                        _log.Info(() => LogEvent.Create(ex));
                        return false;
                    }
                }, _disposeToken.Token);
        }

        private async Task ClientConnectAsync()
        {
            var buffer = new byte[8192];
            while (!_disposeToken.IsCancellationRequested) {
                _log.Debug(() => LogEvent.Create("SERVER: Accepting clients."));
                _client = await _listener.AcceptTcpClientAsync().ConfigureAwait(false);
                _stream = await GetStreamAsync();
                _log.Debug(() => LogEvent.Create("SERVER: Connected client"));

                _connectedTrigger.TrySetResult(true);
                OnConnected?.Invoke();
                _clientSemaphore.Release();
                try {
                    while (!_disposeToken.IsCancellationRequested) {
                        var read = await _stream.ReadAsync(buffer, 0, buffer.Length, _disposeToken.Token).ConfigureAwait(false);
                        if (read > 0 && OnReceivedAsync != null) {
                            await OnReceivedAsync(new ArraySegment<byte>(buffer, 0, read));
                        }
                    }
                } catch (OperationCanceledException) {
                } catch (AggregateException ex) when (ex.InnerException is OperationCanceledException) {
                } catch (Exception ex) {
                    _log.Debug(() => LogEvent.Create(ex, "SERVER: client failed"));
                } finally {
                    DropConnection();
                }
                await _clientSemaphore.WaitAsync(_disposeToken.Token);
                _log.Debug(() => LogEvent.Create("SERVER: Client Disconnected."));
                _connectedTrigger = new TaskCompletionSource<bool>();
                OnDisconnected?.Invoke();
            }
        }

        private async Task<Stream> GetStreamAsync()
        {
            var stream = _client.GetStream();
            if (_certificate == null) return stream;

            var sslStream = new SslStream(
                stream,
                false,
                (sender, certificate, chain, sslPolicyErrors) => true,
                null,
                EncryptionPolicy.RequireEncryption
            );
            await sslStream.AuthenticateAsServerAsync(_certificate).ConfigureAwait(false);
            _log.Debug(() => LogEvent.Create($"SERVER: SSL connection, SslProtocol:{sslStream.SslProtocol}, KeyExchange:{sslStream.KeyExchangeAlgorithm}.{sslStream.KeyExchangeStrength}, Cipher:{sslStream.CipherAlgorithm}.{sslStream.CipherStrength}, Hash:{sslStream.HashAlgorithm}.{sslStream.HashStrength}, Authenticated:{sslStream.IsAuthenticated}, MutuallyAuthenticated:{sslStream.IsMutuallyAuthenticated}, Encrypted:{sslStream.IsEncrypted}, Signed:{sslStream.IsSigned}"));
            return sslStream;
        }

        public void Dispose()
        {
            _disposeToken?.Cancel();

            using (_disposeToken) {
                try {
                    _connectTask?.Wait(TimeSpan.FromSeconds(1));
                } catch (OperationCanceledException) {
                } catch (AggregateException ex) when (ex.InnerException is OperationCanceledException) {
                } catch (Exception ex) {
                    _log.Warn(() => LogEvent.Create(ex));
                }
                _listener.Stop();
            }
        }
    }
}