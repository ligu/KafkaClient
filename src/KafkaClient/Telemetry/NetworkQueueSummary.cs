using System;

namespace KafkaClient.Telemetry
{
    public class NetworkQueueSummary
    {
        public int BytesQueued;
        public double KilobytesQueued => MathHelper.ConvertToKilobytes(BytesQueued);
        public TimeSpan OldestBatchInQueue { get; set; }
        public int QueuedMessages { get; set; }
        public int QueuedBatchCount;
        public int SampleSize { get; set; }
    }
}