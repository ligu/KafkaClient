using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaClient.Common
{
    public static class Extensions
    { 
        /// <summary>
        /// Splits a collection into given batch sizes and returns as an enumerable of batches.
        /// </summary>
        public static IEnumerable<IEnumerable<T>> Batch<T>(this IEnumerable<T> collection, int batchSize)
        {
            var nextbatch = new List<T>(batchSize);
            foreach (T item in collection)
            {
                nextbatch.Add(item);
                if (nextbatch.Count == batchSize)
                {
                    yield return nextbatch;
                    nextbatch = new List<T>(batchSize);
                }
            }
            if (nextbatch.Count > 0)
                yield return nextbatch;
        }

        /// <summary>
        /// Extracts a concrete exception out of a Continue with result.
        /// </summary>
        public static Exception ExtractException(this Task task)
        {
            if (task.IsFaulted == false) return null;
            if (task.Exception != null) return task.Exception.Flatten();
            return new Exception("Unknown exception occured.");
        }

        public static IEnumerable<T> Repeat<T>(this int count, Func<T> producer)
        {
            for (var i = 0; i < count; i++) {
                yield return producer();
            }
        }

        public static Task<T> AttemptAsync<T>(this IRetry policy, Func<int, Stopwatch, Task<T>> action, CancellationToken cancellationToken)
        {
            return policy.AttemptAsync(action, null, cancellationToken);
        }

        public static async Task<T> AttemptAsync<T>(this IRetry policy, Func<int, Stopwatch, Task<T>> action, Action<Exception, int, TimeSpan?> onException, CancellationToken cancellationToken)
        {
            var timer = new Stopwatch();
            timer.Start();
            for (var attempt = 0;; attempt++) {
                try {
                    return await action(attempt, timer).ConfigureAwait(false);
                } catch (Exception ex) {
                    var retryDelay = policy.RetryDelay(attempt, timer.Elapsed);
                    onException?.Invoke(ex, attempt, retryDelay);
                    if (!retryDelay.HasValue) throw;

                    await Task.Delay(retryDelay.Value, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        public static Task<T> AttemptAsync<T>(this IRetry policy, Func<int, Stopwatch, T> action, CancellationToken cancellationToken)
        {
            return policy.AttemptAsync(action, null, cancellationToken);
        }

        public static async Task<T> AttemptAsync<T>(this IRetry policy, Func<int, Stopwatch, T> action, Action<Exception, int, TimeSpan?> onException, CancellationToken cancellationToken)
        {
            var timer = new Stopwatch();
            timer.Start();
            for (var attempt = 0;; attempt++) {
                try {
                    return action(attempt, timer);
                } catch (Exception ex) {
                    var retryDelay = policy.RetryDelay(attempt, timer.Elapsed);
                    onException?.Invoke(ex, attempt, retryDelay);
                    if (!retryDelay.HasValue) throw;

                    await Task.Delay(retryDelay.Value, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        public static T GetValue<T>(this SerializationInfo info, string name)
        {
            return (T)info.GetValue(name, typeof(T));
        }

        public static ImmutableList<T> AddNotNull<T>(this ImmutableList<T> list, T item) where T : class
        {
            return item != null ? list.Add(item) : list;
        }

        public static ImmutableList<T> AddNotNullRange<T>(this ImmutableList<T> list, IEnumerable<T> items)
        {
            return items != null ? list.AddRange(items) : list;
        }
    }
}