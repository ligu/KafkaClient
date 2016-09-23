using System;

namespace KafkaClient.Common
{
    public class BackoffRetry : Retry
    {
        private readonly TimeSpan? _delay;

        public BackoffRetry(TimeSpan timeout, TimeSpan? delay, int? maxAttempts = null)
            : base (timeout, maxAttempts)
        {
            _delay = delay;
        }

        protected override TimeSpan? GetDelay(int attempt, TimeSpan timeTaken)
        {
            if (timeTaken > Timeout) return null;

            double delayMilliseconds = 0;
            if (_delay.HasValue) {
                // multiplied backoff
                delayMilliseconds = _delay.Value.TotalMilliseconds*attempt;
            } else if (attempt > 0) {
                // exponential backoff
                // from: http://alexandrebrisebois.wordpress.com/2013/02/19/calculating-an-exponential-back-off-delay-based-on-failed-attempts/
                delayMilliseconds = 1d / 2d * (Math.Pow(2d, attempt) - 1d);
            }

            return TimeSpan.FromMilliseconds(Math.Min(delayMilliseconds, Timeout.TotalMilliseconds - timeTaken.TotalMilliseconds));
        }
    }
}