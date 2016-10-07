using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// SyncGroupRequest => GroupId GenerationId MemberId GroupAssignment
    ///   GroupId => string
    ///   GenerationId => int32
    ///   MemberId => string
    ///   GroupAssignment => [MemberId MemberAssignment]
    ///     MemberId => string
    ///     MemberAssignment => bytes
    /// 
    /// 
    /// see http://kafka.apache.org/protocol.html#protocol_messages
    /// </summary>
    public class SyncGroupRequest : GroupRequest, IRequest<SyncGroupResponse>, IEquatable<SyncGroupRequest>
    {
        /// <inheritdoc />
        public SyncGroupRequest(string groupId, string memberId, int generationId, IEnumerable<SyncGroupAssignment> groupAssignments) 
            : base(ApiKeyRequestType.SyncGroup, groupId, memberId, generationId)
        {
            GroupAssignments = ImmutableList<SyncGroupAssignment>.Empty.AddNotNullRange(groupAssignments);
        }

        public IImmutableList<SyncGroupAssignment> GroupAssignments { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as SyncGroupRequest);
        }

        /// <inheritdoc />
        public bool Equals(SyncGroupRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                && GroupAssignments.HasEqualElementsInOrder(other.GroupAssignments);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                return (base.GetHashCode()*397) ^ (GroupAssignments?.GetHashCode() ?? 0);
            }
        }

        /// <inheritdoc />
        public static bool operator ==(SyncGroupRequest left, SyncGroupRequest right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(SyncGroupRequest left, SyncGroupRequest right)
        {
            return !Equals(left, right);
        }
    }

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