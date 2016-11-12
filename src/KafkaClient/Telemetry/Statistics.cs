using System;

namespace KafkaClient.Telemetry
{
    public abstract class Statistics
    {
        protected Statistics(DateTime startedAt, TimeSpan duration)
        {
            StartedAt = startedAt;
            EndedAt = startedAt.Add(duration);
        }

        public DateTime StartedAt { get; }
        public DateTime EndedAt { get; }
    }
}