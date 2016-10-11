namespace KafkaClient.Protocol
{
    public class GroupMember : GroupData
    {
        public GroupMember(string memberId, byte[] metadata)
            : base(memberId, metadata)
        {
        }

        public string MemberId => Id;
        public byte[] Metadata => Data;
    }
}