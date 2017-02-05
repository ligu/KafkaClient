using System;

namespace KafkaClient.Common
{
    public class Retry : IRetry
    {
        public static IRetry None { get; } = new Retry(0);

        public static IRetry AtMost(int maxRetries) => new Retry(maxRetries, maximumDelay: TimeSpan.Zero);

        public static IRetry WithBackoff(int maxRetries, TimeSpan? maximum = null, TimeSpan? minimumDelay = null, TimeSpan? maximumDelay = null) => new Retry(maxRetries, maximum, minimumDelay, maximumDelay);

        public static IRetry Until(TimeSpan maximum, TimeSpan? minimumDelay = null, TimeSpan? maximumDelay = null) => new Retry(null, maximum, minimumDelay, maximumDelay);

        private readonly int? _maxRetries;
        private readonly TimeSpan? _maximum;
        private readonly TimeSpan? _maximumDelay;
        private readonly TimeSpan? _minimumDelay;

        internal Retry(int? maxRetries = null, TimeSpan? maximum = null, TimeSpan? minimumDelay = null, TimeSpan? maximumDelay = null)
        {
            _maxRetries = maxRetries;
            _maximum = maximum;
            _maximumDelay = maximumDelay;
            _minimumDelay = minimumDelay;
        }


        public TimeSpan? RetryDelay(int retryAttempt, TimeSpan timeTaken)
        {
            if (_maxRetries.HasValue && _maxRetries.Value <= retryAttempt) return null;

            var remainingMilliseconds = double.MaxValue;
            if (_maximum.HasValue) {
                remainingMilliseconds = Math.Max(0d, _maximum.Value.TotalMilliseconds - timeTaken.TotalMilliseconds);
                if (remainingMilliseconds <= 0.05d) return null;
            }

            var delayMilliseconds = _minimumDelay?.TotalMilliseconds*(retryAttempt + 1) ?? 0d;
            if (_maximumDelay.HasValue && _maximumDelay.Value.TotalMilliseconds < delayMilliseconds) {
                delayMilliseconds = _maximumDelay.Value.TotalMilliseconds;
            }
            if (delayMilliseconds > remainingMilliseconds) return null;

            return TimeSpan.FromMilliseconds(delayMilliseconds);
        }
    }
}