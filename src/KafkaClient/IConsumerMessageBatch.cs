using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Protocol;

namespace KafkaClient
{
    public interface IConsumerMessageBatch : IAsyncDisposable
    {
        IImmutableList<Message> Messages { get; }
        Task CommitAsync(CancellationToken cancellationToken);
    }
}