using System;

namespace KafkaClient.Telemetry
{
    public class ProduceRequestStatistic
    {
        public DateTime CreatedOnUtc { get; }
        public int MessageCount { get; }
        public int MessageBytes { get; }
        public int PayloadBytes { get; }
        public int CompressedBytes { get; }
        public double CompressionRatio { get; }

        public ProduceRequestStatistic(int messageCount, int payloadBytes, int compressedBytes)
        {
            CreatedOnUtc = DateTime.UtcNow;
            MessageCount = messageCount;
            MessageBytes = payloadBytes + compressedBytes;
            PayloadBytes = payloadBytes;
            CompressedBytes = compressedBytes;

            CompressionRatio = MessageBytes == 0 ? 0 : Math.Round((double)compressedBytes / MessageBytes, 4);
        }
    }
}