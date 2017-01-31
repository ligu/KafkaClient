using System;

namespace KafkaClient.Common
{
    public class Retry : IRetry
    {
        public static IRetry None { get; } = new Retry(0);

        public static IRetry AtMost(int maxAttempts) => new Retry(maxAttempts, maximumDelay: TimeSpan.Zero);

        public static IRetry WithBackoff(int maxAttempts, TimeSpan? maximum = null, TimeSpan? minimumDelay = null, TimeSpan? maximumDelay = null) => new Retry(maxAttempts, maximum, minimumDelay, maximumDelay);

        public static IRetry Until(TimeSpan maximum, TimeSpan? minimumDelay = null, TimeSpan? maximumDelay = null) => new Retry(null, maximum, minimumDelay, maximumDelay);

        private readonly int? _maxAttempts;
        private readonly TimeSpan? _maximum;
        private readonly TimeSpan? _maximumDelay;
        private readonly TimeSpan? _minimumDelay;

        internal Retry(int? maxAttempts = null, TimeSpan? maximum = null, TimeSpan? minimumDelay = null, TimeSpan? maximumDelay = null)
        {
            _maxAttempts = maxAttempts;
            _maximum = maximum;
            _maximumDelay = maximumDelay;
            _minimumDelay = minimumDelay;
        }


        public TimeSpan? RetryDelay(int attempt, TimeSpan timeTaken)
        {
            if (_maxAttempts.HasValue && _maxAttempts.Value <= attempt) return null;

            var remainingMilliseconds = double.MaxValue;
            if (_maximum.HasValue) {
                remainingMilliseconds = Math.Max(0d, _maximum.Value.TotalMilliseconds - timeTaken.TotalMilliseconds);
                if (remainingMilliseconds <= 0.05d) return null;
            }

            var delayMilliseconds = _minimumDelay?.TotalMilliseconds*(attempt + 1) ?? 0d;
            if (_maximumDelay.HasValue && _maximumDelay.Value.TotalMilliseconds < delayMilliseconds) {
                delayMilliseconds = _maximumDelay.Value.TotalMilliseconds;
            }
            if (delayMilliseconds > remainingMilliseconds) return null;

            return TimeSpan.FromMilliseconds(delayMilliseconds);
        }
    }
}