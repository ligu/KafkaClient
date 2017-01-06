using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Protocol;

namespace KafkaClient
{
    public interface IConsumerMessageBatch
    {
        IImmutableList<Message> Messages { get; }
        Task CommitAsync(Message lastSuccessful, CancellationToken cancellationToken);
        Task<IConsumerMessageBatch> FetchNextAsync(int maxCount, CancellationToken cancellationToken);
    }
}