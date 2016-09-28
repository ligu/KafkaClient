using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Runtime.ExceptionServices;
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
        /// Attempts to prepare the exception for re-throwing by preserving the stack trace. The returned exception should be immediately thrown.
        /// </summary>
        /// <param name="exception">The exception. May not be <c>null</c>.</param>
        /// <returns>The <see cref="Exception"/> that was passed into this method.</returns>
        public static Exception PrepareForRethrow(this Exception exception)
        {
            ExceptionDispatchInfo.Capture(exception).Throw();

            // The code cannot ever get here. We just return a value to work around a badly-designed API (ExceptionDispatchInfo.Throw):
            //  https://connect.microsoft.com/VisualStudio/feedback/details/689516/exceptiondispatchinfo-api-modifications (http://www.webcitation.org/6XQ7RoJmO)
            return exception;
        }

        public static IEnumerable<T> Repeat<T>(this int count, Func<T> producer)
        {
            for (var i = 0; i < count; i++) {
                yield return producer();
            }
        }

        public static async Task<T> AttemptAsync<T>(
            this IRetry policy, 
            Func<int, Stopwatch, Task<RetryAttempt<T>>> action, 
            Action<int, TimeSpan> onRetry, 
            Action<int> onFinal, 
            Action<Exception, int, TimeSpan> onException, 
            Action<Exception, int> onFinalException, 
            CancellationToken cancellationToken)
        {
            var timer = new Stopwatch();
            timer.Start();
            for (var attempt = 0;; attempt++) {
                cancellationToken.ThrowIfCancellationRequested();
                try {
                    var response = await action(attempt, timer).ConfigureAwait(false);
                    if (response.IsSuccessful) return response.Value;

                    var retryDelay = policy.RetryDelay(attempt, timer.Elapsed);
                    if (retryDelay.HasValue) {
                        onRetry?.Invoke(attempt, retryDelay.Value);
                        await Task.Delay(retryDelay.Value, cancellationToken).ConfigureAwait(false);
                    } else {
                        onFinal?.Invoke(attempt);
                        return response.Value;
                    }
                } catch (Exception ex) {
                    await policy.HandleErrorAndDelayAsync(onException, onFinalException, attempt, timer, ex, cancellationToken);
                }
            }
        }

        private static async Task HandleErrorAndDelayAsync(
            this IRetry policy, 
            Action<Exception, int, TimeSpan> onException, 
            Action<Exception, int> onFinalException, 
            int attempt, 
            Stopwatch timer, 
            Exception ex, 
            CancellationToken cancellationToken)
        {
            var retryDelay = policy.RetryDelay(attempt, timer.Elapsed);
            if (retryDelay.HasValue) {
                onException?.Invoke(ex, attempt, retryDelay.Value);
                await Task.Delay(retryDelay.Value, cancellationToken).ConfigureAwait(false);
            } else {
                onFinalException?.Invoke(ex, attempt);
                throw ex.PrepareForRethrow();
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