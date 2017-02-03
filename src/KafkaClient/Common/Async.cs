using System;
using System.Threading.Tasks;

namespace KafkaClient.Common
{
    /// <summary>
    /// Extensions for async code, to be called statically or as an extension.
    /// </summary>
    public static class Async
    {
        //public static async Task Using<T>(T disposable, Action<T> action) where T : IAsyncDisposable
        //{
        //    try {
        //        action(disposable);
        //    } finally {
        //        await disposable.DisposeAsync().ConfigureAwait(false);
        //    }
        //}

        public static async Task Using<T>(T disposable, Func<T, Task> asyncAction) where T : IAsyncDisposable
        {
            try {
                await asyncAction(disposable).ConfigureAwait(false);
            } finally {
                await disposable.DisposeAsync().ConfigureAwait(false);
            }
        }

        public static async Task UsingAsync<T>(this T disposable, Action action) where T : IAsyncDisposable
        {
            try {
                action();
            } finally {
                await disposable.DisposeAsync().ConfigureAwait(false);
            }
        }

        public static async Task UsingAsync<T>(this T disposable, Func<Task> asyncAction) where T : IAsyncDisposable
        {
            try {
                await asyncAction().ConfigureAwait(false);
            } finally {
                await disposable.DisposeAsync().ConfigureAwait(false);
            }
        }
    }
}