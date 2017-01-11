using System;
using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Protocol;

namespace KafkaClient
{
    public interface IMessageBatch : IDisposable
    {
        IImmutableList<Message> Messages { get; }
        Action OnDisposed { get; set; }
        void MarkSuccessful(Message message);
        Task<long> CommitMarkedAsync(CancellationToken cancellationToken);
        Task<IMessageBatch> FetchNextAsync(CancellationToken cancellationToken);
    }
}