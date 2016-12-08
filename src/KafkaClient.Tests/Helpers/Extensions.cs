using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Protocol;

namespace KafkaClient.Tests.Helpers
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

        public static async Task TemporaryTopicAsync(this IRouter router, Func<string, Task> asyncAction, [CallerMemberName] string name = null)
        {
            var topicName = TestConfig.TopicName();
            try {
                await router.SendToAnyAsync(new CreateTopicsRequest(new [] { new CreateTopicsRequest.Topic(topicName, 1, 1) }, TimeSpan.FromSeconds(1)), CancellationToken.None);
            } catch (RequestException ex) when (ex.ErrorCode == ErrorResponseCode.TopicAlreadyExists) {
                // ignore
            }
            try {
                await asyncAction(topicName);
            } finally {
                await router.SendToAnyAsync(new DeleteTopicsRequest(new [] { topicName }, TimeSpan.FromSeconds(1)), CancellationToken.None);
            }
        }
    }
}