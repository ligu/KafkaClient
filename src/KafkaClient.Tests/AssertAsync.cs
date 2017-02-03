using System;
using System.Diagnostics;
using System.Threading.Tasks;
using NUnit.Framework;

namespace KafkaClient.Tests
{
    public static class AssertAsync
    {
        /// <exception cref="Exception">A delegate callback throws an exception.</exception>
        public static async Task<bool> EventuallyThat(Func<bool> predicate, int milliSeconds = 3000)
        {
            var sw = Stopwatch.StartNew();
            while (predicate() == false) {
                if (sw.ElapsedMilliseconds > milliSeconds) return false;
                await Task.Delay(50).ConfigureAwait(false);
            }
            return true;
        }

        public static async Task Throws<T>(Func<Task> asyncAction) where T : Exception
        {
            try {
                await asyncAction();
                Assert.Fail($"Should have thrown {nameof(T)}");
            } catch (T) {
                // expected
            }
        }
    }
}