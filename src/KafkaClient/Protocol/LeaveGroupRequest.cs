using System;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// LeaveGroupRequest => group_id member_id 
    ///   group_id => STRING           -- The group id.
    ///   member_id => STRING          -- The member id assigned by the group coordinator.
    /// 
    /// see http://kafka.apache.org/protocol.html#protocol_messages
    /// 
    /// To explicitly leave a group, the client can send a leave group request. This is preferred over letting the session timeout expire since 
    /// it allows the group to rebalance faster, which for the consumer means that less time will elapse before partitions can be reassigned to 
    /// an active member.
    /// </summary>
    public class LeaveGroupRequest : Request, IRequest<LeaveGroupResponse>, IGroupMember, IEquatable<LeaveGroupRequest>
    {
        public override string ToString() => $"{{Api:{ApiKey},group_id:{group_id},member_id:{member_id}}}";

        public override string ShortString() => $"{ApiKey} {group_id} {member_id}";

        protected override void EncodeBody(IKafkaWriter writer, IRequestContext context)
        {
            writer.Write(group_id)
                  .Write(member_id);
        }

        /// <inheritdoc />
        public LeaveGroupRequest(string groupId, string memberId) : base(ApiKey.LeaveGroup)
        {
            group_id = groupId;
            member_id = memberId;
        }

        /// <inheritdoc />
        public string group_id { get; }

        /// <inheritdoc />
        public string member_id { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as LeaveGroupRequest);
        }

        /// <inheritdoc />
        public bool Equals(LeaveGroupRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                   && string.Equals(group_id, other.group_id) 
                   && string.Equals(member_id, other.member_id);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                int hashCode = base.GetHashCode();
                hashCode = (hashCode*397) ^ (group_id?.GetHashCode() ?? 0);
                hashCode = (hashCode*397) ^ (member_id?.GetHashCode() ?? 0);
                return hashCode;
            }
        }
        
        #endregion
    }
}