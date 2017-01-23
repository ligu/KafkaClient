using System;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaClient.Common
{
    public static class SemaphoreExtensions
    {
        public static void Lock(this SemaphoreSlim semaphore, Action action, CancellationToken cancellationToken)
        {
            semaphore.Wait(cancellationToken);
            try {
                action();
            } finally {
                semaphore.Release(1);
            }
        }

        public static T Lock<T>(this SemaphoreSlim semaphore, Func<T> function, CancellationToken cancellationToken)
        {
            semaphore.Wait(cancellationToken);
            try {
                return function();
            } finally {
                semaphore.Release(1);
            }
        }

        public static async Task LockAsync(this SemaphoreSlim semaphore, Action action, CancellationToken cancellationToken)
        {
            await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            try {
                action();
            } finally {
                semaphore.Release(1);
            }
        }

        public static async Task<T> LockAsync<T>(this SemaphoreSlim semaphore, Func<T> function, CancellationToken cancellationToken)
        {
            await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            try {
                return function();
            } finally {
                semaphore.Release(1);
            }
        }

        public static async Task LockAsync(this SemaphoreSlim semaphore, Func<Task> asyncAction, CancellationToken cancellationToken)
        {
            await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            try {
                await asyncAction();
            } finally {
                semaphore.Release(1);
            }
        }

        public static async Task<T> LockAsync<T>(this SemaphoreSlim semaphore, Func<Task<T>> asyncFunction, CancellationToken cancellationToken)
        {
            await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            try {
                return await asyncFunction();
            } finally {
                semaphore.Release(1);
            }
        }
    }
}