using System;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaClient.Common
{
    /// <summary>
    /// Utility functions for dealing with Task's.
    /// </summary>
    /// <remarks>
    /// Some of these come from orleans TaskExtensions here: https://github.com/dotnet/orleans/blob/master/src/Orleans/Async/TaskExtensions.cs#L218
    /// Others come from Stephen Cleary here: https://github.com/StephenCleary/AsyncEx.Tasks/blob/master/src/Nito.AsyncEx.Tasks/SynchronousTaskExtensions.cs
    /// </remarks>
    public static class TaskExtensions
    {
        /// <summary>
        /// Execute an await task while monitoring a given cancellation token.  Use with non-cancelable async operations.
        /// </summary>
        /// <remarks>
        /// This extension method will only cancel the await and not the actual IO operation.  The status of the IO opperation will still
        /// need to be considered after the operation is cancelled.
        /// See <see cref="http://blogs.msdn.com/b/pfxteam/archive/2012/10/05/how-do-i-cancel-non-cancelable-async-operations.aspx"/>
        /// </remarks>
        public static async Task<T> WithCancellation<T>(this Task<T> task, CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<bool>();
            using (cancellationToken.Register(_ => ((TaskCompletionSource<bool>)_).TrySetResult(true), tcs)) {
                if (task != await Task.WhenAny(task, tcs.Task).ConfigureAwait(false)) {
                    throw new OperationCanceledException(cancellationToken);
                }
            }
            return await task.ConfigureAwait(false);
        }

        /// <summary>
        /// Execute an await task while monitoring a given cancellation token.  Use with non-cancelable async operations.
        /// </summary>
        /// <remarks>
        /// This extension method will only cancel the await and not the actual IO operation.  The status of the IO opperation will still
        /// need to be considered after the operation is cancelled.
        /// See <see cref="http://blogs.msdn.com/b/pfxteam/archive/2012/10/05/how-do-i-cancel-non-cancelable-async-operations.aspx"/>
        /// </remarks>
        public static async Task WithCancellation(this Task task, CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<bool>();
            using (cancellationToken.Register(_ => ((TaskCompletionSource<bool>)_).TrySetResult(true), tcs)) {
                if (task != await Task.WhenAny(task, tcs.Task).ConfigureAwait(false)) {
                    throw new OperationCanceledException(cancellationToken);
                }
            }
        }

        public static async Task<bool> WithCancellationBool(this Task task, CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<bool>();
            using (cancellationToken.Register(_ => ((TaskCompletionSource<bool>)_).TrySetResult(true), tcs)) {
                if (task != await Task.WhenAny(task, tcs.Task).ConfigureAwait(false)) {
                    return false;
                }
            }
            return true;
        }

        /// <summary>
        /// Waits for the task to complete, unwrapping any exceptions.
        /// </summary>
        /// <param name="task">The task. May not be <c>null</c>.</param>
        public static void WaitAndUnwrapException(this Task task)
        {
            if (task == null) throw new ArgumentNullException(nameof(task));

            task.GetAwaiter().GetResult();
        }

        /// <summary>
        /// Waits for the task to complete, unwrapping any exceptions.
        /// </summary>
        /// <param name="task">The task. May not be <c>null</c>.</param>
        /// <param name="cancellationToken">A cancellation token to observe while waiting for the task to complete.</param>
        /// <exception cref="OperationCanceledException">The <paramref name="cancellationToken"/> was cancelled before the <paramref name="task"/> completed, or the <paramref name="task"/> raised an <see cref="OperationCanceledException"/>.</exception>
        public static void WaitAndUnwrapException(this Task task, CancellationToken cancellationToken)
        {
            if (task == null) throw new ArgumentNullException(nameof(task));

            try {
                task.Wait(cancellationToken);
            } catch (AggregateException ex) {
                throw ex.InnerException.PrepareForRethrow();
            }
        }

        /// <summary>
        /// Waits for the task to complete, unwrapping any exceptions.
        /// </summary>
        /// <typeparam name="TResult">The type of the result of the task.</typeparam>
        /// <param name="task">The task. May not be <c>null</c>.</param>
        /// <returns>The result of the task.</returns>
        public static TResult WaitAndUnwrapException<TResult>(this Task<TResult> task)
        {
            if (task == null) throw new ArgumentNullException(nameof(task));

            return task.GetAwaiter().GetResult();
        }

        /// <summary>
        /// Waits for the task to complete, unwrapping any exceptions.
        /// </summary>
        /// <typeparam name="TResult">The type of the result of the task.</typeparam>
        /// <param name="task">The task. May not be <c>null</c>.</param>
        /// <param name="cancellationToken">A cancellation token to observe while waiting for the task to complete.</param>
        /// <returns>The result of the task.</returns>
        /// <exception cref="OperationCanceledException">The <paramref name="cancellationToken"/> was cancelled before the <paramref name="task"/> completed, or the <paramref name="task"/> raised an <see cref="OperationCanceledException"/>.</exception>
        public static TResult WaitAndUnwrapException<TResult>(this Task<TResult> task, CancellationToken cancellationToken)
        {
            if (task == null) throw new ArgumentNullException(nameof(task));

            try {
                task.Wait(cancellationToken);
                return task.Result;
            } catch (AggregateException ex) {
                throw ex.InnerException.PrepareForRethrow();
            }
        }

        /// <summary>
        /// Waits for the task to complete, but does not raise task exceptions. The task exception (if any) is unobserved.
        /// </summary>
        /// <param name="task">The task. May not be <c>null</c>.</param>
        public static void WaitWithoutException(this Task task)
        {
            if (task == null) throw new ArgumentNullException(nameof(task));

            try {
                task.Wait();
            } catch (AggregateException) {
            }
        }

        /// <summary>
        /// Waits for the task to complete, but does not raise task exceptions. The task exception (if any) is unobserved.
        /// </summary>
        /// <param name="task">The task. May not be <c>null</c>.</param>
        /// <param name="cancellationToken">A cancellation token to observe while waiting for the task to complete.</param>
        /// <exception cref="OperationCanceledException">The <paramref name="cancellationToken"/> was cancelled before the <paramref name="task"/> completed.</exception>
        public static void WaitWithoutException(this Task task, CancellationToken cancellationToken)
        {
            if (task == null) throw new ArgumentNullException(nameof(task));

            try {
                task.Wait(cancellationToken);
            } catch (AggregateException) {
                cancellationToken.ThrowIfCancellationRequested();
            }
        }
    }
}
