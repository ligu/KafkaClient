using System;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaClient.Connections
{
    /// <summary>
    /// The transport represents the lowest level TCP stream connection to a Kafka server.
    /// The Read and Write deal with bytes over the network transport, and coordination of them must be managed externally.
    /// </summary>
    public interface ITransport : IDisposable
    {
        Task ConnectAsync(CancellationToken cancellationToken);

        Task<int> ReadBytesAsync(byte[] buffer, int bytesToRead, Action<int> onBytesRead, CancellationToken cancellationToken);

        Task<int> WriteBytesAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken, int correlationId = 0);
    }
}