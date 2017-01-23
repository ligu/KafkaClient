using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaClient.Common
{
    public static class RetryExtensions
    {
        public static IEnumerable<T> Repeat<T>(this int count, Func<T> producer)
        {
            for (var i = 0; i < count; i++) {
                yield return producer();
            }
        }

        public static IEnumerable<T> Repeat<T>(this int count, Func<int, T> producer)
        {
            for (var i = 0; i < count; i++) {
                yield return producer(i);
            }
        }

        public static async Task AttemptAsync(
            this IRetry policy, 
            Func<int, Task> action, 
            Action<Exception, int, TimeSpan> onException, 
            Action<Exception, int> onFinalException, 
            CancellationToken cancellationToken)
        {
            var timer = new Stopwatch();
            timer.Start();
            for (var attempt = 0; !cancellationToken.IsCancellationRequested; attempt++) {
                cancellationToken.ThrowIfCancellationRequested();
                try {
                    await action(attempt).ConfigureAwait(false);
                    // reset attempt when successful
                    attempt = -1;
                    timer.Restart();
                } catch (Exception ex) {
                    if (attempt == 0) { // first failure
                        timer.Restart();
                    }
                    await policy.HandleErrorAndDelayAsync(onException, onFinalException, attempt, timer, ex, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        public static async Task<T> AttemptAsync<T>(
            this IRetry policy, 
            Func<int, Stopwatch, Task<RetryAttempt<T>>> action, 
            Action<int, TimeSpan> onRetry, 
            Action<int> onFinal, 
            Action<Exception> onException, 
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
                    onException?.Invoke(ex);
                    onFinal?.Invoke(attempt);
                    return default(T);
                }
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
                    await policy.HandleErrorAndDelayAsync(onException, onFinalException, attempt, timer, ex, cancellationToken).ConfigureAwait(false);
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
        }    }
}