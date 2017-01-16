using System;
using System.Threading;

namespace KafkaClient.Telemetry
{
    public class ConnectionStatistics : Statistics
    {
        public ConnectionStatistics(DateTimeOffset startedAt, TimeSpan duration)
            : base(startedAt, duration)
        {
        }

        private int _attempted;
        public int Attempted => _attempted;

        public void Attempt()
        {
            Interlocked.Increment(ref _attempted);
        }

        private int _successes;
        public int Successes => _successes;

        private long _duration;
        public TimeSpan Duration => TimeSpan.FromTicks(_duration);

        public void Success(TimeSpan duration)
        {
            Interlocked.Increment(ref _successes);
            Interlocked.Add(ref _duration, duration.Ticks);
        }

        private int _failures;
        public int Failures => _failures;

        public void Failure()
        {
            Interlocked.Increment(ref _failures);
        }
    }
}