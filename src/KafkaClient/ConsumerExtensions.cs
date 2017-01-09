using System;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Assignment;
using KafkaClient.Protocol;

namespace KafkaClient
{
    public static class ConsumerExtensions
    {
        public static Task<IConsumerMessageBatch> FetchBatchAsync(this IConsumer consumer, OffsetResponse.Topic offset, int maxCount, CancellationToken cancellationToken)
        {
            return consumer.FetchBatchAsync(offset.TopicName, offset.PartitionId, offset.Offset, maxCount, cancellationToken);
        }

        public static Task<int> FetchAsync(this IConsumer consumer, OffsetResponse.Topic offset, int batchSize, Func<IConsumerMessageBatch, Task> onMessagesAsync, CancellationToken cancellationToken)
        {
            return consumer.FetchAsync(offset.TopicName, offset.PartitionId, offset.Offset, batchSize, onMessagesAsync, cancellationToken);
        }

        public static Task<int> FetchAsync(this IConsumer consumer, OffsetResponse.Topic offset, int batchSize, Func<Message, Task> onMessageAsync, CancellationToken cancellationToken)
        {
            return consumer.FetchAsync(offset.TopicName, offset.PartitionId, offset.Offset, batchSize, onMessageAsync, cancellationToken);
        }

        public static Task<int> FetchAsync(this IConsumer consumer, string topicName, int partitionId, long offset, int batchSize, Func<Message, Task> onMessageAsync, CancellationToken cancellationToken)
        {
            return consumer.FetchAsync(
                topicName, partitionId, offset, batchSize,
                async batch => {
                    foreach (var message in batch.Messages) {
                        await onMessageAsync(message);
                    }
                }, cancellationToken);
        }

        public static async Task<int> FetchAsync(this IConsumer consumer, string topicName, int partitionId, long offset, int batchSize, Func<IConsumerMessageBatch, Task> onMessagesAsync, CancellationToken cancellationToken)
        {
            var total = 0;
            while (!cancellationToken.IsCancellationRequested) {
                var fetched = await consumer.FetchBatchAsync(topicName, partitionId, offset + total, batchSize, cancellationToken);
                await onMessagesAsync(fetched);
                total += fetched.Messages.Count;
            }
            return total;
        }

        public static Task<IConsumerGroupMember> JoinConsumerGroupAsync(this IConsumer consumer, string groupId, ConsumerProtocolMetadata metadata, CancellationToken cancellationToken)
        {
            return consumer.JoinConsumerGroupAsync(groupId, ConsumerEncoder.Protocol, new[] { metadata }, cancellationToken);
        }

        public static Task<IConsumerGroupMember> JoinConsumerGroupAsync(this IConsumer consumer, string groupId, string protocolType, IMemberMetadata metadata, CancellationToken cancellationToken)
        {
            return consumer.JoinConsumerGroupAsync(groupId, protocolType, new[] { metadata }, cancellationToken);
        }

        public static Task CommitAsync(this IConsumerMessageBatch batch, CancellationToken cancellationToken)
        {
            if (batch.Messages.Count == 0) return Task.FromResult(0);

            return batch.CommitAsync(batch.Messages[batch.Messages.Count - 1], cancellationToken);
        }

        public static Task CommitAsync(this IConsumerMessageBatch batch, Message message, CancellationToken cancellationToken)
        {
            batch.MarkSuccessful(message);
            return batch.CommitMarkedAsync(cancellationToken);
        }
    }
}