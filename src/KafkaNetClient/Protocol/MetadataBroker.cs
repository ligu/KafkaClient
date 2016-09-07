namespace KafkaNet.Protocol
{
    public class MetadataBroker : Broker
    {
        public MetadataBroker(int brokerId, string host, int port)
            : base(brokerId, host, port)
        {
        }
    }
}