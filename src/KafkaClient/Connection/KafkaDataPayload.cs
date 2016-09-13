using KafkaClient.Protocol;

namespace KafkaClient.Connection
{
    public class KafkaDataPayload
    {
        public KafkaDataPayload(byte[] buffer, int correlationId = 0, ApiKeyRequestType apiKey = ApiKeyRequestType.Produce, int messageCount = 0)
        {
            Buffer = buffer;
            CorrelationId = correlationId;
            ApiKey = apiKey;
            MessageCount = messageCount;
        }

        public byte[] Buffer { get; }
        public int CorrelationId { get; }
        public ApiKeyRequestType ApiKey { get; }
        public int MessageCount { get; }

        public bool TrackPayload => MessageCount > 0;
    }
}