using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Assignment;
using KafkaClient.Common;
using KafkaClient.Protocol;

namespace KafkaClient
{
    public static class ConsumerExtensions
    {
        public static Task<IMessageBatch> FetchBatchAsync(this IConsumer consumer, OffsetResponse.Topic offset, int maxCount, CancellationToken cancellationToken)
        {
            return consumer.FetchBatchAsync(offset.TopicName, offset.PartitionId, offset.Offset, maxCount, cancellationToken);
        }

        public static Task<int> FetchAsync(this IConsumer consumer, OffsetResponse.Topic offset, int batchSize, Func<IMessageBatch, Task> onMessagesAsync, CancellationToken cancellationToken)
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

        public static async Task<int> FetchAsync(this IConsumer consumer, string topicName, int partitionId, long offset, int batchSize, Func<IMessageBatch, Task> onMessagesAsync, CancellationToken cancellationToken)
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

        public static async Task<IImmutableList<IMessageBatch>> FetchBatchesAsync(this IConsumerGroupMember member, int maxCount, CancellationToken cancellationToken)
        {
            var batches = new List<IMessageBatch>();
            IMessageBatch batch;
            while (!(batch = await member.FetchBatchAsync(maxCount, cancellationToken)).IsEmpty()) {
                batches.Add(batch);
            }
            return batches.ToImmutableList();
        }

        public static async Task ProcessMessagesAsync(this IConsumerGroupMember member, Func<IMessageBatch, CancellationToken, Task> processBatch, int count, ILog log, CancellationToken cancellationToken)
        {
            var tasks = new List<Task>();
            while (!cancellationToken.IsCancellationRequested) {
                var batches = await member.FetchBatchesAsync(count, cancellationToken);
                tasks.AddRange(batches.Select(async batch => await batch.ProcessMessagesAsync(processBatch, log, cancellationToken)));
                if (tasks.Count == 0) break;
                await Task.WhenAny(tasks);
                tasks = tasks.Where(t => !t.IsCompleted).ToList();
            }
        }

        public static Task CommitAsync(this IMessageBatch batch, CancellationToken cancellationToken)
        {
            if (batch.Messages.Count == 0) return Task.FromResult(0);

            return batch.CommitAsync(batch.Messages[batch.Messages.Count - 1], cancellationToken);
        }

        public static Task CommitAsync(this IMessageBatch batch, Message message, CancellationToken cancellationToken)
        {
            batch.MarkSuccessful(message);
            return batch.CommitMarkedAsync(cancellationToken);
        }

        public static bool IsEmpty(this IMessageBatch batch)
        {
            return batch?.Messages?.Count == 0;
        }

        public static async Task ProcessMessagesAsync(this IMessageBatch batch, Func<IMessageBatch, CancellationToken, Task> processBatch, ILog log, CancellationToken cancellationToken)
        {
            try {
                do {
                    await processBatch(batch, cancellationToken);
                    batch = await batch.FetchNextAsync(cancellationToken);
                } while (!batch.IsEmpty() && !cancellationToken.IsCancellationRequested);
            } catch (ObjectDisposedException ex) {
                log.Info(() => LogEvent.Create(ex));
            } catch (TaskCanceledException ex) {
                log.Debug(() => LogEvent.Create(ex));
            } catch (OperationCanceledException ex) {
                log.Debug(() => LogEvent.Create(ex));
            } catch (Exception ex) {
                log.Error(LogEvent.Create(ex));
                throw;
            }
        }
    }
}