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
        public SyncGroupRequest(string groupId, int generationId, string memberId, IEnumerable<SyncGroupAssignment> groupAssignments) 
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
}