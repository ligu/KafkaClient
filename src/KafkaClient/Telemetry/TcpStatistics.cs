using System;
using System.Threading;

namespace KafkaClient.Telemetry
{
    public class TcpStatistics : Statistics
    {
        public TcpStatistics(DateTimeOffset startedAt, TimeSpan duration)
            : base(startedAt, duration)
        {
        }

        private int _attempted;
        public int Attempted => _attempted;

        private int _bytesAttempted;
        public int BytesAttempted => _bytesAttempted;

        public void Attempt(int bytes = 0)
        {
            Interlocked.Increment(ref _attempted);
            Interlocked.Add(ref _bytesAttempted, bytes);
        }

        private int _started;
        public int Started => _started;

        private int _bytesStarted;
        public int BytesStarted => _bytesStarted;

        public void Start(int bytes)
        {
            Interlocked.Increment(ref _started);
            Interlocked.Add(ref _bytesStarted, bytes);
        }

        public void Partial(int bytes)
        {
            Interlocked.Add(ref _bytesStarted, -bytes);
        }

        private int _successes;
        public int Successes => _successes;

        private int _bytesSuccessful;
        public int BytesSuccessful => _bytesSuccessful;

        public void Success(TimeSpan duration, int bytes)
        {
            Interlocked.Increment(ref _successes);
            Interlocked.Add(ref _duration, duration.Ticks);
            Interlocked.Add(ref _bytesSuccessful, bytes);
        }

        private long _duration;
        public TimeSpan Duration => TimeSpan.FromTicks(_duration);

        private int _failures;
        public int Failures => _failures;

        public void Failure(TimeSpan duration)
        {
            Interlocked.Increment(ref _failures);
            Interlocked.Add(ref _duration, duration.Ticks);
        }
    }
}