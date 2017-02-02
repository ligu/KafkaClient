using System;
using System.IO;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Connections;
using Nito.AsyncEx;

namespace KafkaClient.Testing
{
    public class TcpServer : IDisposable
    {
        private readonly ISslConfiguration _sslConfiguration;
        private readonly X509Certificate _certificate;
        public Func<ArraySegment<byte>, Task> OnReceivedAsync { get; set; }
        public Action OnConnected { get; set; }
        public Action OnDisconnected { get; set; }

        private readonly TcpListener _listener;
        private readonly CancellationTokenSource _disposeToken = new CancellationTokenSource();
        private readonly Task _connectTask = null;
        private TaskCompletionSource<bool> _connectedTrigger = new TaskCompletionSource<bool>();

        private readonly ILog _log;

        public TcpServer(int port, ILog log = null, ISslConfiguration sslConfiguration = null, X509Certificate certificate = null)
        {
            _sslConfiguration = sslConfiguration;
            _certificate = certificate;
            _log = log ?? new TraceLog(LogLevel.Error);
            _listener = new TcpListener(IPAddress.Any, port);
            _listener.Start();

            _connectTask = Task.Run(ClientConnectAsync, _disposeToken.Token);
        }

        public async Task<bool> SendDataAsync(ArraySegment<byte> data)
        {
            try {
                await _clientSemaphore.WaitAsync();
                _log.Debug(() => LogEvent.Create($"SERVER: writing {data.Count} bytes."));
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
                _log.Debug(() => LogEvent.Create("SERVER: Accepting clients."));
                _client = await _listener.AcceptTcpClientAsync().ConfigureAwait(false);
                _log.Debug(() => LogEvent.Create("SERVER: Connected client"));

                _connectedTrigger.TrySetResult(true);
                OnConnected?.Invoke();
                _clientSemaphore.Release();
                Stream stream = null;
                try {
                    stream = _client.GetStream();
                    if (_sslConfiguration != null && _certificate != null) {
                        stream = new SslStream(
                            stream,
                            false,
                            _sslConfiguration.RemoteCertificateValidationCallback,
                            _sslConfiguration.LocalCertificateSelectionCallback,
                            _sslConfiguration.EncryptionPolicy ?? EncryptionPolicy.RequireEncryption
                        );
                        await ((SslStream)stream).AuthenticateAsServerAsync(_certificate).ConfigureAwait(false);
                    }

                    while (!_disposeToken.IsCancellationRequested) {
                        var read = await stream.ReadAsync(buffer, 0, buffer.Length, _disposeToken.Token)
                                                .ThrowIfCancellationRequested(_disposeToken.Token)
                                                .ConfigureAwait(false);
                        if (read > 0 && OnReceivedAsync != null) {
                            await OnReceivedAsync(new ArraySegment<byte>(buffer, 0, read));
                        }
                    }
                } catch (Exception ex) {
                    if (!(ex is OperationCanceledException)) {
                        _log.Debug(() => LogEvent.Create(ex, "SERVER: client failed"));
                    }
                } finally {
                    stream?.Dispose();
                    _client?.Dispose();
                }

                _log.Debug(() => LogEvent.Create("SERVER: Client Disconnected."));
                await _clientSemaphore.WaitAsync(_disposeToken.Token); //remove the one client
                _connectedTrigger = new TaskCompletionSource<bool>();
                OnDisconnected?.Invoke();
            }
        }

        public void Dispose()
        {
            _disposeToken?.Cancel();

            using (_disposeToken) {
                try {
                    _connectTask?.Wait(TimeSpan.FromSeconds(1));
                } catch (Exception ex) {
                    _log.Warn(() => LogEvent.Create(ex));
                }
                _listener.Stop();
            }
        }
    }
}