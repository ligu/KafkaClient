namespace KafkaClient.Protocol
{
    public class GroupProtocol : GroupData
    {
        public GroupProtocol(string name, byte[] metadata)
            : base(name, metadata)
        {
        }

        public string Name => Id;
        public byte[] Metadata => Data;
    }
}