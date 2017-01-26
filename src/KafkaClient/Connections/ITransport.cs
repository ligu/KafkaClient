using System;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaClient.Connections
{
    public interface ITransport : IDisposable
    {
        Task ConnectAsync(CancellationToken cancellationToken);

        Task<int> ReadBytesAsync(byte[] buffer, int bytesToRead, Action<int> onBytesRead, CancellationToken cancellationToken);

        Task<int> WriteBytesAsync(int correlationId, ArraySegment<byte> buffer, CancellationToken cancellationToken);
    }
}