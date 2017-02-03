using System;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Threading.Tasks;
using AgileObjects.ReadableExpressions;
using NUnit.Framework;

namespace KafkaClient.Tests
{
    public static class AssertAsync
    {
        /// <exception cref="Exception">A delegate callback throws an exception.</exception>
        public static async Task<bool> ThatEventually(Expression<Func<bool>> predicate, TimeSpan? timeout = null)
        {
            var compiled = predicate.Compile();
            var timer = Stopwatch.StartNew();
            var timeoutMilliseconds = timeout?.TotalMilliseconds ?? 3000;
            while (compiled() == false) {
                if (timer.ElapsedMilliseconds > timeoutMilliseconds) Assert.Fail(predicate.ToReadableString());
                await Task.Delay(50).ConfigureAwait(false);
            }
            return true;
        }

        public static async Task Throws<T>(Func<Task> asyncAction, Func<T, bool> when = null) where T : Exception
        {
            try {
                await asyncAction();
                Assert.Fail($"Should have thrown {typeof(T)}");
            } catch (T ex) {
                if (when != null) {
                    Assert.That(when(ex));
                }
            }
        }
    }
}