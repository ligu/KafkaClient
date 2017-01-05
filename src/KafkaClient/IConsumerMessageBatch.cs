using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Protocol;

namespace KafkaClient
{
    public interface IConsumerMessageBatch
    {
        IImmutableList<Message> Messages { get; }
        Task CommitAsync(CancellationToken cancellationToken);
        Task CommitAsync(Message lastSuccessful, CancellationToken cancellationToken);
    }
}