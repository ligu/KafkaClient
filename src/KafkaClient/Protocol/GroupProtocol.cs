namespace KafkaClient.Protocol
{
    public class GroupProtocol
    {
        public GroupProtocol(string name, byte[] metadata)
        {
            Name = name;
            Metadata = metadata;
        }

        public string Name { get; }
        public byte[] Metadata { get; }
    }
}