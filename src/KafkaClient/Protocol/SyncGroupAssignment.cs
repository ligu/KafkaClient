namespace KafkaClient.Protocol
{
    public class SyncGroupAssignment
    {
        public SyncGroupAssignment(string memberId, byte[] memberAssignment)
        {
            MemberId = memberId;
            MemberAssignment = memberAssignment;
        }

        public string MemberId { get; }
        public byte[] MemberAssignment { get; }
    }
}