namespace KafkaClient.Telemetry
{
    public class ProduceRequestSummary
    {
        public int SampleSize;
        public int MessageCount;
        public int MessagesPerSecond;
        public int MessageBytesPerSecond;
        public double MessageKilobytesPerSecond => MathHelper.ConvertToKilobytes(MessageBytesPerSecond);
        public int PayloadBytesPerSecond;
        public double PayloadKilobytesPerSecond => MathHelper.ConvertToKilobytes(PayloadBytesPerSecond);
        public int CompressedBytesPerSecond;
        public double CompressedKilobytesPerSecond => MathHelper.ConvertToKilobytes(CompressedBytesPerSecond);
        public double AverageCompressionRatio;
    }
}