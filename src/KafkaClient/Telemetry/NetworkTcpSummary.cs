using System;

namespace KafkaClient.Telemetry
{
    public class NetworkTcpSummary
    {
        public int MessagesPerSecond;
        public int MaxMessagesPerSecond;
        public int BytesPerSecond;
        public TimeSpan AverageWriteDuration;
        public double KilobytesPerSecond => MathHelper.ConvertToKilobytes(BytesPerSecond);
        public TimeSpan AverageTotalDuration { get; set; }
        public int SampleSize { get; set; }
        public int MessagesLastBatch { get; set; }
    }
}