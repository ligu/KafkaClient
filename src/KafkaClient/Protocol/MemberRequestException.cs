namespace KafkaClient.Protocol
{
    /// <summary>
    /// An exception caused by a Kafka Request for groups (JoinGroup, SyncGroup, etc)
    /// </summary>
    public class MemberRequestException : RequestException
    {
        public MemberRequestException(IGroupMember member, ApiKey apiKey, ErrorResponseCode errorCode)
            : base(apiKey, errorCode, member != null ? $"{{GroupId:{member.GroupId},MemberId:{member.MemberId}}}" : null)
        {
            Member = member;
        }

        public IGroupMember Member { get; }
    }
}