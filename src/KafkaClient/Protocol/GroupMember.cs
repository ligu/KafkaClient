using System;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// TODO: how is this distinct from GroupProtocol ? 
    /// </summary>
    public class GroupMember : IEquatable<GroupMember>
    {
        private static readonly byte[] Empty = {};

        public GroupMember(string memberId, byte[] metadata)
        {
            MemberId = memberId;
            Metadata = metadata ?? Empty;
        }

        public string MemberId { get; }
        public byte[] Metadata { get; }

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
                && Metadata.HasEqualElementsInOrder(other.Metadata);
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