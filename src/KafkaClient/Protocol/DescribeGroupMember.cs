using System;
using KafkaClient.Common;
using KafkaClient.Protocol.Types;

namespace KafkaClient.Protocol
{
    public class DescribeGroupMember : IEquatable<DescribeGroupMember>
    {
        public DescribeGroupMember(string memberId, string clientId, string clientHost, IMemberMetadata memberMetadata, IMemberAssignment memberAssignment)
        {
            MemberId = memberId;
            ClientId = clientId;
            ClientHost = clientHost;
            MemberMetadata = memberMetadata;
            MemberAssignment = memberAssignment;
        }

        /// <summary>
        /// The memberId assigned by the coordinator
        /// </summary>
        public string MemberId { get; }

        /// <summary>
        /// The client id used in the member's latest join group request
        /// </summary>
        public string ClientId { get; }

        /// <summary>
        /// The client host used in the request session corresponding to the member's join group.
        /// </summary>
        public string ClientHost { get; }

        /// <summary>
        /// The metadata corresponding to the current group protocol in use (will only be present if the group is stable).
        /// </summary>
        public IMemberMetadata MemberMetadata { get; }

        /// <summary>
        /// The current assignment provided by the group leader (will only be present if the group is stable).
        /// </summary>
        public IMemberAssignment MemberAssignment { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as DescribeGroupMember);
        }

        /// <inheritdoc />
        public bool Equals(DescribeGroupMember other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return string.Equals(MemberId, other.MemberId) 
                   && string.Equals(ClientId, other.ClientId) 
                   && string.Equals(ClientHost, other.ClientHost) 
                   && Equals(MemberMetadata, other.MemberMetadata) 
                   && Equals(MemberAssignment, other.MemberMetadata);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = MemberId?.GetHashCode() ?? 0;
                hashCode = (hashCode*397) ^ (ClientId?.GetHashCode() ?? 0);
                hashCode = (hashCode*397) ^ (ClientHost?.GetHashCode() ?? 0);
                hashCode = (hashCode*397) ^ (MemberMetadata?.GetHashCode() ?? 0);
                hashCode = (hashCode*397) ^ (MemberAssignment?.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        /// <inheritdoc />
        public static bool operator ==(DescribeGroupMember left, DescribeGroupMember right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(DescribeGroupMember left, DescribeGroupMember right)
        {
            return !Equals(left, right);
        }
    }
}