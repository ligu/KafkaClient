using System;
using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Protocol;

namespace KafkaClient
{
    public interface IConsumerMessageBatch : IDisposable
    {
        IImmutableList<Message> Messages { get; }
        void MarkSuccessful(Message message);
        Task<long> CommitMarkedAsync(CancellationToken cancellationToken);
        Task<IConsumerMessageBatch> FetchNextAsync(int maxCount, CancellationToken cancellationToken);
    }
}