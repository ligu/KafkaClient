using System;
using KafkaClient.Protocol.Types;

namespace KafkaClient.Protocol
{
    public class GroupMember : IEquatable<GroupMember>
    {
        public GroupMember(string memberId, IMemberMetadata metadata)
        {
            MemberId = memberId;
            Metadata = metadata;
        }

        public string MemberId { get; }
        public IMemberMetadata Metadata { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as GroupMember);
        }

        /// <inheritdoc />
        public bool Equals(GroupMember other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return string.Equals(MemberId, other.MemberId) 
                && Equals(Metadata, other.Metadata);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                return ((MemberId?.GetHashCode() ?? 0)*397) ^ (Metadata?.GetHashCode() ?? 0);
            }
        }

        /// <inheritdoc />
        public static bool operator ==(GroupMember left, GroupMember right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(GroupMember left, GroupMember right)
        {
            return !Equals(left, right);
        }
    }
}