using System;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaClient.Common
{
    public static class SemaphoreExtensions
    {
        public static void Lock(this SemaphoreSlim semaphore, Action action, CancellationToken cancellationToken)
        {
            try {
                semaphore.Wait(cancellationToken);
            } catch (ArgumentNullException ex) {
                throw new ObjectDisposedException(nameof(semaphore), ex);
            }
            try {
                action();
            } finally {
                semaphore.Release(1);
            }
        }

        public static T Lock<T>(this SemaphoreSlim semaphore, Func<T> function, CancellationToken cancellationToken)
        {
            try {
                semaphore.Wait(cancellationToken);
            } catch (ArgumentNullException ex) {
                throw new ObjectDisposedException(nameof(semaphore), ex);
            }
            try {
                return function();
            } finally {
                semaphore.Release(1);
            }
        }

        public static async Task LockAsync(this SemaphoreSlim semaphore, Action action, CancellationToken cancellationToken)
        {
            try {
                await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            } catch (ArgumentNullException ex) {
                throw new ObjectDisposedException(nameof(semaphore), ex);
            }
            try {
                action();
            } finally {
                semaphore.Release(1);
            }
        }

        public static async Task<T> LockAsync<T>(this SemaphoreSlim semaphore, Func<T> function, CancellationToken cancellationToken)
        {
            try {
                await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            } catch (ArgumentNullException ex) {
                throw new ObjectDisposedException(nameof(semaphore), ex);
            }
            try {
                return function();
            } finally {
                semaphore.Release(1);
            }
        }

        public static async Task LockAsync(this SemaphoreSlim semaphore, Func<Task> asyncAction, CancellationToken cancellationToken)
        {
            try {
                await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            } catch (ArgumentNullException ex) {
                throw new ObjectDisposedException(nameof(semaphore), ex);
            }
            try {
                await asyncAction();
            } finally {
                semaphore.Release(1);
            }
        }

        public static async Task<T> LockAsync<T>(this SemaphoreSlim semaphore, Func<Task<T>> asyncFunction, CancellationToken cancellationToken)
        {
            try {
                await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            } catch (ArgumentNullException ex) {
                throw new ObjectDisposedException(nameof(semaphore), ex);
            }
            try {
                return await asyncFunction();
            } finally {
                semaphore.Release(1);
            }
        }
    }
}