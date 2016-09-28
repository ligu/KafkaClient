using System;
using System.Threading.Tasks;

namespace KafkaClient.Tests
{
    public static class Extensions
    {
        /// <summary>
        /// Mainly used for testing, allows waiting on a single task without throwing exceptions.
        /// </summary>
        public static void SafeWait(this Task task, TimeSpan timeout)
        {
            try {
                task.Wait(timeout);
            } catch {
                // ignore an exception that happens in this source
            }
        }        
    }
}