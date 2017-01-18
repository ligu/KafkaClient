namespace KafkaClient.Connections
{
    public class DataPayload
    {
        public DataPayload(byte[] buffer)
        {
            Buffer = buffer;
        }

        public byte[] Buffer { get; }
    }
}