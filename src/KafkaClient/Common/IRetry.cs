using System;

namespace KafkaClient.Common
{
    public interface IRetry
    {
        /// <summary>
        /// How much to delay before the next attempt.
        /// </summary>
        /// <param name="attempt">The attempt number.</param>
        /// <param name="timeTaken">How much time has already been taken.</param>
        /// <returns>null if no more attempts should be made</returns>
        TimeSpan? RetryDelay(int attempt, TimeSpan timeTaken);
    }
}