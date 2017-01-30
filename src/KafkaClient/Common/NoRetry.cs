using System;

namespace KafkaClient.Common
{
    public class NoRetry : IRetry
    {
        public TimeSpan? RetryDelay(int attempt, TimeSpan timeTaken) => null;

        public bool ShouldRetry(int attempt, TimeSpan timeTaken) => false;
    }
}