using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Protocol;
using NUnit.Framework;

namespace KafkaClient.Tests
{
    public static class Extensions
    {
        /// <summary>
        /// Mainly used for testing, allows waiting on a single task without throwing exceptions.
        /// </summary>
        public static void SafeWait(this Task task, TimeSpan timeout)
        {
            try {
                task.Wait(timeout);
            } catch {
                // ignore an exception that happens in this source
            }
        }

        public static async Task TemporaryTopicAsync(this IRouter router, Func<string, Task> asyncAction, int partitions = 1, [CallerMemberName] string name = null)
        {
            var topicName = TestConfig.TopicName(name);
            try {
                await router.SendToAnyAsync(new CreateTopicsRequest(new [] { new CreateTopicsRequest.Topic(topicName, partitions, 1) }, TimeSpan.FromSeconds(1)), CancellationToken.None);
            } catch (RequestException ex) when (ex.ErrorCode == ErrorResponseCode.TopicAlreadyExists) {
                // ignore already exists
            }
            try {
                await asyncAction(topicName);
            } finally {
                // right now deleting the topic isn't propagating properly, so subsequent runs of the test fail
                // await router.SendToAnyAsync(new DeleteTopicsRequest(new [] { topicName }, TimeSpan.FromSeconds(1)), CancellationToken.None);
            }
        }

        public static async Task DeleteTopicAsync(this IRouter router, [CallerMemberName] string name = null)
        {
            var topicName = TestConfig.TopicName(name);
            try {
                var response = await router.SendToAnyAsync(new DeleteTopicsRequest(new [] { topicName }, TimeSpan.FromMilliseconds(500)), CancellationToken.None);
                if (response.Errors.Any(e => e == ErrorResponseCode.RequestTimedOut)) {
                    Assert.Inconclusive("Cannot validate when topic remains");
                }
            } catch (RequestException ex) when (ex.ErrorCode == ErrorResponseCode.TopicAlreadyExists) {
                // ignore already exists
            }
        }

        public static async Task CommitTopicOffsetAsync(this IRouter router, string topicName, int partitionId, string groupId, long offset, CancellationToken cancellationToken)
        {
            var request = new OffsetCommitRequest(groupId, new [] { new OffsetCommitRequest.Topic(topicName, partitionId, offset) });
            await router.SendAsync(request, topicName, partitionId, cancellationToken).ConfigureAwait(false);
        }

        public static Task<IMessageBatch> FetchBatchAsync(this IConsumer consumer, OffsetResponse.Topic offset, int batchSize, CancellationToken cancellationToken)
        {
            return consumer.FetchBatchAsync(offset.TopicName, offset.PartitionId, offset.Offset, cancellationToken, batchSize);
        }

        public static Task<int> FetchAsync(this IConsumer consumer, Func<IMessageBatch, CancellationToken, Task> onMessagesAsync, OffsetResponse.Topic offset, int batchSize, CancellationToken cancellationToken)
        {
            return consumer.FetchAsync(onMessagesAsync, offset.TopicName, offset.PartitionId, offset.Offset, cancellationToken, batchSize);
        }

        public static Task<int> FetchAsync(this IConsumer consumer, Func<Message, CancellationToken, Task> onMessageAsync, OffsetResponse.Topic offset, int batchSize, CancellationToken cancellationToken)
        {
            return consumer.FetchAsync(onMessageAsync, offset.TopicName, offset.PartitionId, offset.Offset, cancellationToken, batchSize);
        }
    }
}