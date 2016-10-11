namespace KafkaClient.Protocol
{
    public class SyncGroupAssignment : GroupData
    {
        public SyncGroupAssignment(string memberId, byte[] memberAssignment)
            : base(memberId, memberAssignment)
        {
        }

        public string MemberId => Id;
        public byte[] MemberAssignment => Data;
    }
}