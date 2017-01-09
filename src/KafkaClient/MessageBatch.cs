using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Protocol;

namespace KafkaClient
{
    public class MessageBatch : IConsumerMessageBatch
    {
        public static readonly MessageBatch Empty = new MessageBatch();

        private MessageBatch()
        {
        }

        public void Dispose()
        {
        }

        public IImmutableList<Message> Messages => ImmutableList<Message>.Empty;

        public void MarkSuccessful(Message message)
        {
        }

        public Task<long> CommitMarkedAsync(CancellationToken cancellationToken)
        {
            return Task.FromResult(0L);
        }

        public Task<IConsumerMessageBatch> FetchNextAsync(int maxCount, CancellationToken cancellationToken)
        {
            return Task.FromResult((IConsumerMessageBatch)Empty);
        }
    }
}