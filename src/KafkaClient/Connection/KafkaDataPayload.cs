using KafkaClient.Protocol;

namespace KafkaClient.Connection
{
    public class KafkaDataPayload
    {
        public int CorrelationId { get; set; }
        public ApiKeyRequestType ApiKey { get; set; }
        public int MessageCount { get; set; }

        public bool TrackPayload => MessageCount > 0;

        public byte[] Buffer { get; set; }
    }
}