using System;
using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Protocol;

namespace KafkaClient
{
    public class MessageBatch : IConsumerMessageBatch, IEquatable<MessageBatch>
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

        public override bool Equals(object obj)
        {
            return Equals(obj as MessageBatch);
        }

        public bool Equals(MessageBatch other)
        {
            return ReferenceEquals(this, other);
        }

        public override int GetHashCode()
        {
            return 0;
        }

        public static bool operator ==(MessageBatch left, MessageBatch right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(MessageBatch left, MessageBatch right)
        {
            return !Equals(left, right);
        }
    }
}