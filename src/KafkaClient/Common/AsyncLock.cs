using System;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaClient.Common
{
    /// <summary>
    /// An asynchronous locking construct.
    /// </summary>
    /// <remarks>
    /// This is based on Stephen Toub's implementation here: http://blogs.msdn.com/b/pfxteam/archive/2012/02/12/10266988.aspx
    /// However, we're using SemaphoreSlim as the basis rather than AsyncSempahore, since in .NET 4.5 SemaphoreSlim implements the WaitAsync() method.
    /// </remarks>
    public class AsyncLock : IDisposable
    {
        private readonly SemaphoreSlim _semaphore;
        private readonly Task<IDisposable> _releaser;

        public AsyncLock()
        {
            _semaphore = new SemaphoreSlim(1, 1);
            _releaser = Task.FromResult<IDisposable>(new Releaser(this));
        }

        public Task<IDisposable> LockAsync(CancellationToken cancellationToken)
        {
            var wait = _semaphore.WaitAsync(cancellationToken);

            const string canceledMessage = "Unable to aquire lock within timeout alloted.";
            if (!wait.IsCompleted) {
                return wait.ContinueWith(
                    (continued, state) => {
                        if (continued.IsCanceled) throw new OperationCanceledException(canceledMessage);
                        return (IDisposable) new Releaser((AsyncLock) state);
                    },
                    this, cancellationToken, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
            }
            if (wait.IsCanceled) throw new OperationCanceledException(canceledMessage);
            return _releaser;
        }

        public Task<IDisposable> LockAsync() => LockAsync(CancellationToken.None);

        /// <summary>
        /// Synchronously acquires the lock. Returns a disposable that releases the lock when disposed. This method may block the calling thread.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token used to cancel the lock. If this is already set, then this method will attempt to take the lock immediately (succeeding if the lock is currently available).</param>
        public IDisposable Lock(CancellationToken cancellationToken)
        {
            return LockAsync(cancellationToken).WaitAndUnwrapException();
        }

        /// <summary>
        /// Synchronously acquires the lock. Returns a disposable that releases the lock when disposed. This method may block the calling thread.
        /// </summary>
        public IDisposable Lock() => Lock(CancellationToken.None);

        private int _disposeCount = 0;

        public void Dispose()
        {
            if (Interlocked.Increment(ref _disposeCount) != 1) return;

            using (_semaphore) { }
            using (_releaser) { }
        }

        private struct Releaser : IDisposable
        {
            private readonly AsyncLock _toRelease;

            internal Releaser(AsyncLock toRelease)
            {
                _toRelease = toRelease;
            }

            public void Dispose()
            {
                _toRelease?._semaphore.Release();
            }
        }
    }
}