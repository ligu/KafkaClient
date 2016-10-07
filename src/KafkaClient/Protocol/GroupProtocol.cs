namespace KafkaClient.Protocol
{
    public abstract class GroupProtocol
    {
        protected GroupProtocol(string name)
        {
            Name = name;
        }

        public string Name { get; }
        public abstract byte[] Metadata { get; }
    }
}