namespace KafkaClient.Protocol
{
    /// <summary>
    /// TODO: how is this distinct from GroupProtocol ? 
    /// </summary>
    public class GroupMember
    {
        public GroupMember(string memberId, byte[] metadata)
        {
            MemberId = memberId;
            Metadata = metadata;
        }

        public string MemberId { get; }
        public byte[] Metadata { get; }
    }
}