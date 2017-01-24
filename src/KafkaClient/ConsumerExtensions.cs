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
        public static async Task<IConsumer> CreateConsumerAsync(this KafkaOptions options)
        {
            return new Consumer(await options.CreateRouterAsync(), options.ConsumerConfiguration, options.ConnectionConfiguration.Encoders, false);
        }

        public static Task<int> FetchAsync(this IConsumer consumer, Func<Message, CancellationToken, Task> onMessageAsync, string topicName, int partitionId, long offset, CancellationToken cancellationToken, int? batchSize = null)
        {
            return consumer.FetchAsync(async (batch, token) => {
                foreach (var message in batch.Messages) {
                    await onMessageAsync(message, token).ConfigureAwait(false);
                }
            }, topicName, partitionId, offset, cancellationToken, batchSize);
        }

        public static async Task<int> FetchAsync(this IConsumer consumer, Func<IMessageBatch, CancellationToken, Task> onMessagesAsync, string topicName, int partitionId, long offset, CancellationToken cancellationToken, int? batchSize = null)
        {
            var total = 0;
            while (!cancellationToken.IsCancellationRequested) {
                var fetched = await consumer.FetchBatchAsync(topicName, partitionId, offset + total, cancellationToken, batchSize).ConfigureAwait(false);
                await onMessagesAsync(fetched, cancellationToken).ConfigureAwait(false);
                total += fetched.Messages.Count;
            }
            return total;
        }

        public static Task<IConsumerMember> JoinConsumerGroupAsync(this IConsumer consumer, string groupId, ConsumerProtocolMetadata metadata, CancellationToken cancellationToken)
        {
            return consumer.JoinGroupAsync(groupId, ConsumerEncoder.Protocol, new[] { metadata }, cancellationToken);
        }

        public static Task<IConsumerMember> JoinConsumerGroupAsync(this IConsumer consumer, string groupId, string protocolType, IMemberMetadata metadata, CancellationToken cancellationToken)
        {
            return consumer.JoinGroupAsync(groupId, protocolType, new[] { metadata }, cancellationToken);
        }

        public static async Task<IImmutableList<IMessageBatch>> FetchBatchesAsync(this IConsumerMember member, CancellationToken cancellationToken, int? batchSize = null)
        {
            var batches = new List<IMessageBatch>();
            IMessageBatch batch;
            while (!(batch = await member.FetchBatchAsync(cancellationToken, batchSize).ConfigureAwait(false)).IsEmpty()) {
                batches.Add(batch);
            }
            return batches.ToImmutableList();
        }

        public static async Task FetchUntilDisposedAsync(this IConsumerMember member, Func<IMessageBatch, CancellationToken, Task> onMessagesAsync, CancellationToken cancellationToken, int? batchSize = null)
        {
            try {
                await member.FetchAsync(onMessagesAsync, cancellationToken, batchSize);
            } catch (ObjectDisposedException) {
                // ignore
            }
        }

        public static async Task FetchAsync(this IConsumerMember member, Func<IMessageBatch, CancellationToken, Task> onMessagesAsync, CancellationToken cancellationToken, int? batchSize = null)
        {
            var tasks = new List<Task>();
            while (!cancellationToken.IsCancellationRequested) {
                var batches = await member.FetchBatchesAsync(cancellationToken, batchSize).ConfigureAwait(false);
                tasks.AddRange(batches.Select(async batch => await batch.FetchAsync(onMessagesAsync, member.Log, cancellationToken).ConfigureAwait(false)));
                if (tasks.Count == 0) break;
                await Task.WhenAny(tasks).ConfigureAwait(false);
                tasks = tasks.Where(t => !t.IsCompleted).ToList();
            }
        }

        public static async Task<long> CommitMarkedIgnoringDisposedAsync(this IMessageBatch batch, CancellationToken cancellationToken)
        {
            try {
                return await batch.CommitMarkedAsync(cancellationToken);
            } catch (ObjectDisposedException) {
                // ignore
                return 0;
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

        public static async Task FetchAsync(this IMessageBatch batch, Func<IMessageBatch, CancellationToken, Task> onMessagesAsync, ILog log, CancellationToken cancellationToken)
        {
            try {
                do {
                    using (var source = new CancellationTokenSource()) {
                        batch.OnDisposed = source.Cancel;
                        using (cancellationToken.Register(source.Cancel)) {
                            await onMessagesAsync(batch, source.Token).ConfigureAwait(false);
                        }
                        batch.OnDisposed = null;
                    }
                    batch = await batch.FetchNextAsync(cancellationToken).ConfigureAwait(false);
                } while (!batch.IsEmpty() && !cancellationToken.IsCancellationRequested);
            } catch (ObjectDisposedException ex) {
                log.Info(() => LogEvent.Create(ex));
            } catch (OperationCanceledException ex) {
                log.Debug(() => LogEvent.Create(ex));
            } catch (Exception ex) {
                log.Error(LogEvent.Create(ex));
                throw;
            }
        }
    }
}