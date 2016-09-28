using System;
using System.Diagnostics.CodeAnalysis;
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
        /// Observes and ignores a potential exception on a given Task.
        /// If a Task fails and throws an exception which is never observed, it will be caught by the .NET finalizer thread.
        /// This function awaits the given task and if the exception is thrown, it observes this exception and simply ignores it.
        /// This will prevent the escalation of this exception to the .NET finalizer thread.
        /// </summary>
        /// <param name="task">The task to be ignored.</param>
        [SuppressMessage("Microsoft.Performance", "CA1804:RemoveUnusedLocals", MessageId = "ignored")]
        [SuppressMessage("ReSharper", "UnusedVariable")]
        public static void Ignore(this Task task)
        {
            if (task.IsCompleted) {
                var ignored = task.Exception;
            } else {
                task.ContinueWith(
                    t => { var ignored = t.Exception; },
                    CancellationToken.None,
                    TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default);
            }
        }

        /// <summary>
        /// This will apply a timeout delay to the task, allowing us to exit early
        /// </summary>
        /// <param name="taskToComplete">The task we will timeout after timeSpan</param>
        /// <param name="timeout">Amount of time to wait before timing out</param>
        /// <exception cref="TimeoutException">If we time out we will get this exception</exception>
        /// <returns>The value of the completed task</returns>
        public static async Task<T> WithTimeout<T>(this Task<T> taskToComplete, TimeSpan timeout)
        {
            if (taskToComplete.IsCompleted) {
                return await taskToComplete;
            }

            var timeoutCancellationTokenSource = new CancellationTokenSource();
            var completedTask = await Task.WhenAny(taskToComplete, Task.Delay(timeout, timeoutCancellationTokenSource.Token));

            // We got done before the timeout, or were able to complete before this code ran, return the result
            if (taskToComplete == completedTask) {
                timeoutCancellationTokenSource.Cancel();
                // Await this so as to propagate the exception correctly
                return await taskToComplete;
            }

            // We did not complete before the timeout, we fire and forget to ensure we observe any exceptions that may occur
            taskToComplete.Ignore();
            throw new TimeoutException($"WithTimeout has timed out after {timeout}.");
        }

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

            var cancelRegistration = cancellationToken.Register(source => ((TaskCompletionSource<bool>)source).TrySetResult(true), tcs);

            using (cancelRegistration) {
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

            var cancelRegistration = cancellationToken.Register(source => ((TaskCompletionSource<bool>)source).TrySetResult(true), tcs);

            using (cancelRegistration) {
                if (task != await Task.WhenAny(task, tcs.Task).ConfigureAwait(false)) {
                    throw new OperationCanceledException(cancellationToken);
                }
            }
        }

        public static async Task<bool> WithCancellationBool(this Task task, CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<bool>();

            var cancelRegistration = cancellationToken.Register(source => ((TaskCompletionSource<bool>)source).TrySetResult(true), tcs);

            using (cancelRegistration) {
                if (task != await Task.WhenAny(task, tcs.Task).ConfigureAwait(false)) {
                    return false;
                }
            }
            return true;
        }

        /// <summary>
        /// Mainly used for testing, allows waiting on a single task without throwing exceptions.
        /// </summary>
        public static void SafeWait(this Task source, TimeSpan timeout)
        {
            try {
                source.Wait(timeout);
            } catch {
                // ignore an exception that happens in this source
            }
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
