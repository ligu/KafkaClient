using System;
using KafkaClient.Connections;

namespace KafkaClient.Telemetry
{
    public class NetworkWriteStatistic
    {
        public DateTime CreatedOnUtc { get; }
        public DateTime CompletedOnUtc { get; private set; }
        public bool IsCompleted { get; private set; }
        public bool IsFailed { get; private set; }
        public Endpoint Endpoint { get; }
        public DataPayload Payload { get; }
        public TimeSpan TotalDuration => (IsCompleted ? CompletedOnUtc : DateTime.UtcNow) - CreatedOnUtc;
        public TimeSpan WriteDuration { get; private set; }

        public NetworkWriteStatistic(Endpoint endpoint, DataPayload payload)
        {
            CreatedOnUtc = DateTime.UtcNow;
            Endpoint = endpoint;
            Payload = payload;
        }

        public void SetCompleted(long milliseconds, bool failedFlag)
        {
            IsCompleted = true;
            IsFailed = failedFlag;
            CompletedOnUtc = DateTime.UtcNow;
            WriteDuration = TimeSpan.FromMilliseconds(milliseconds);
        }

        public void SetSuccess(bool failed)
        {
            IsFailed = failed;
        }
    }
}