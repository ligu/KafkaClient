using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Assignment;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// SyncGroupRequest => GroupId GroupGenerationId MemberId [GroupAssignment]
    ///   GroupId => string
    ///   GroupGenerationId => int32
    ///   MemberId => string
    ///   GroupAssignment => MemberId MemberAssignment -- only the leader should specify group assignments
    ///     MemberId => string
    ///     MemberAssignment => bytes
    /// 
    /// see http://kafka.apache.org/protocol.html#protocol_messages
    /// </summary>
    public class SyncGroupRequest : GroupRequest, IRequest<SyncGroupResponse>, IEquatable<SyncGroupRequest>
    {
        public override string ToString() => $"{{Api:{ApiKey},GroupId:{GroupId},MemberId:{MemberId},GenerationId:{GenerationId},GroupAssignments:[{GroupAssignments.ToStrings()}]}}";

        public override string ShortString() => $"{ApiKey} {GroupId} {MemberId}";

        /// <inheritdoc />
        public SyncGroupRequest(string groupId, int generationId, string memberId, IEnumerable<GroupAssignment> groupAssignments = null) 
            : base(ApiKey.SyncGroup, groupId, memberId, generationId)
        {
            GroupAssignments = ImmutableList<GroupAssignment>.Empty.AddNotNullRange(groupAssignments);
        }

        public IImmutableList<GroupAssignment> GroupAssignments { get; }

        #region Equality

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
                return (base.GetHashCode()*397) ^ (GroupAssignments?.Count.GetHashCode() ?? 0);
            }
        }

        #endregion

        public class GroupAssignment : IEquatable<GroupAssignment>
        {
            public override string ToString() => $"{{MemberId:{MemberId},MemberAssignment:{MemberAssignment}}}";

            public GroupAssignment(string memberId, IMemberAssignment memberAssignment)
            {
                MemberId = memberId;
                MemberAssignment = memberAssignment;
            }

            public string MemberId { get; }
            public IMemberAssignment MemberAssignment { get; }

            #region Equality

            /// <inheritdoc />
            public override bool Equals(object obj)
            {
                return Equals(obj as GroupAssignment);
            }

            /// <inheritdoc />
            public bool Equals(GroupAssignment other)
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

            #endregion
        }
    }
}