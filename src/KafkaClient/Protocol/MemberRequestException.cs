using KafkaClient.Connections;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// An exception caused by a Kafka Request for groups (JoinGroup, SyncGroup, etc)
    /// </summary>
    public class MemberRequestException : RequestException
    {
        public MemberRequestException(IGroupMember member, ApiKey apiKey, ErrorCode errorCode, Endpoint endpoint)
            : base(apiKey, errorCode, endpoint, member != null ? $"{{GroupId:{member.group_id},MemberId:{member.member_id}}}" : null)
        {
            _member = member;
        }

        // ReSharper disable once NotAccessedField.Local -- for debugging
        private readonly IGroupMember _member;
    }
}