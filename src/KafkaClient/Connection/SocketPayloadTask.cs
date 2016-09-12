using System;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaClient.Connection
{
    internal class SocketPayloadTask<T> : IDisposable
    {
        public SocketPayloadTask(CancellationToken cancellationToken)
        {
            Tcp = new TaskCompletionSource<T>();
            CancellationToken = cancellationToken;
            _cancellationTokenRegistration = cancellationToken.Register(() => Tcp.TrySetCanceled());
        }

        public CancellationToken CancellationToken { get; }
        public TaskCompletionSource<T> Tcp { get; }

        private readonly CancellationTokenRegistration _cancellationTokenRegistration;

        public void Dispose()
        {
            _cancellationTokenRegistration.Dispose();
        }
    }
}