using System;
using KafkaClient.Protocol.Types;

namespace KafkaClient.Protocol
{
    public class SyncGroupAssignment : IEquatable<SyncGroupAssignment>
    {
        public SyncGroupAssignment(string memberId, IMemberAssignment memberAssignment)
        {
            MemberId = memberId;
            MemberAssignment = memberAssignment;
        }

        public string MemberId { get; }
        public IMemberAssignment MemberAssignment { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as SyncGroupAssignment);
        }

        /// <inheritdoc />
        public bool Equals(SyncGroupAssignment other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return string.Equals(MemberId, other.MemberId) 
                && Equals(MemberAssignment, other.MemberAssignment);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                return ((MemberId?.GetHashCode() ?? 0)*397) ^ (MemberAssignment?.GetHashCode() ?? 0);
            }
        }

        /// <inheritdoc />
        public static bool operator ==(SyncGroupAssignment left, SyncGroupAssignment right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(SyncGroupAssignment left, SyncGroupAssignment right)
        {
            return !Equals(left, right);
        }
    }
}