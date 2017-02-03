using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Assignment;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// DescribeGroupsResponse => [Group]
    ///  Group => ErrorCode GroupId State ProtocolType Protocol [Member]
    ///   ErrorCode => int16
    ///   GroupId => string
    ///   State => string
    ///   ProtocolType => string
    ///   Protocol => string
    ///   Member => MemberId ClientId ClientHost MemberMetadata MemberAssignment
    ///     MemberId => string
    ///     ClientId => string
    ///     ClientHost => string
    ///     MemberMetadata => bytes
    ///     MemberAssignment => bytes
    ///
    /// From http://kafka.apache.org/protocol.html#protocol_messages
    /// </summary>
    public class DescribeGroupsResponse : IResponse, IEquatable<DescribeGroupsResponse>
    {
        public override string ToString() => $"{{Groups:[{Groups.ToStrings()}]}}";

        public DescribeGroupsResponse(IEnumerable<Group> groups)
        {
            Groups = ImmutableList<Group>.Empty.AddNotNullRange(groups);
            Errors = ImmutableList<ErrorCode>.Empty.AddRange(Groups.Select(g => g.ErrorCode));
        }

        public IImmutableList<ErrorCode> Errors { get; }

        public IImmutableList<Group> Groups { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as DescribeGroupsResponse);
        }

        /// <inheritdoc />
        public bool Equals(DescribeGroupsResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Groups.HasEqualElementsInOrder(other.Groups);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return Groups?.Count.GetHashCode() ?? 0;
        }

        /// <inheritdoc />
        public static bool operator ==(DescribeGroupsResponse left, DescribeGroupsResponse right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(DescribeGroupsResponse left, DescribeGroupsResponse right)
        {
            return !Equals(left, right);
        }

        #endregion

        public class Group : IEquatable<Group>
        {
            public override string ToString() => $"{{ErrorCode:{ErrorCode},GroupId:{GroupId},State:{State},ProtocolType:{ProtocolType},Protocol:{Protocol},Members:[{Members.ToStrings()}]}}";

            public Group(ErrorCode errorCode, string groupId, string state, string protocolType, string protocol, IEnumerable<Member> members)
            {
                ErrorCode = errorCode;
                GroupId = groupId;
                State = state;
                ProtocolType = protocolType;
                Protocol = protocol;
                Members = ImmutableList<Member>.Empty.AddNotNullRange(members);
            }

            public ErrorCode ErrorCode { get; }
            public string GroupId { get; }

            /// <summary>
            /// State machine for Coordinator
            /// 
            /// See https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Client-side+Assignment+Proposal
            /// </summary>
            /// <remarks>
            ///               +----------------------------------+
            ///               |             [Down]               |
            ///           +---> There are no active members and  |
            ///           |   | group state has been cleaned up. |
            ///           |   +----------------+-----------------+
            ///  Timeout  |                    |
            ///  expires  |                    | JoinGroup/Heartbeat
            ///  with     |                    | received
            ///  no       |                    v
            ///  group    |   +----------------+-----------------+
            ///  activity |   |           [Initialize]           | 
            ///           |   | The coordinator reads group data |
            ///           |   | in order to transition groups    +---v JoinGroup/Heartbeat  
            ///           |   | from failed coordinators. Any    |   | return               
            ///           |   | heartbeat or join group requests |   | coordinator not ready
            ///           |   | are returned with an error       +---v                      
            ///           |   | indicating that the coordinator  |    
            ///           |   | is not ready yet.                |
            ///           |   +----------------+-----------------+
            ///           |                    |
            ///           |                    | After reading
            ///           |                    | group state
            ///           |                    v
            ///           |   +----------------+-----------------+
            ///           |   |             [Stable]             |
            ///           |   | The coordinator either has an    |
            ///           +---+ active generation or has no      +---v 
            ///               | members and is awaiting the first|   | Heartbeat/SyncGroup
            ///               | JoinGroup. Heartbeats are        |   | from
            ///               | accepted from members in this    |   | active generation
            ///           +---> state and are used to keep group +---v
            ///           |   | members active or to indicate    |
            ///           |   | that they need to join the group |
            ///           |   +----------------+-----------------+
            ///           |                    |
            ///           |                    | JoinGroup
            ///           |                    | received
            ///           |                    v
            ///           |   +----------------+-----------------+
            ///           |   |            [Joining]             |
            ///           |   | The coordinator has received a   |
            ///           |   | JoinGroup request from at least  |
            ///           |   | one member and is awaiting       |
            ///           |   | JoinGroup requests from the rest |
            ///           |   | of the group. Heartbeats or      |
            ///           |   | SyncGroup requests in this state |
            ///           |   | return an error indicating that  |
            ///           |   | a rebalance is in progress.      |
            /// Leader    |   +----------------+-----------------+
            /// SyncGroup |                    |
            /// or        |                    | JoinGroup received
            /// session   |                    | from all members
            /// timeout   |                    v
            ///           |   +----------------+-----------------+
            ///           |   |            [AwaitSync]           |
            ///           |   | The join group phase is complete |
            ///           |   | (all expected group members have |
            ///           |   | sent JoinGroup requests) and the |
            ///           +---+ coordinator is awaiting group    |
            ///               | state from the leader. Unexpected|
            ///               | coordinator requests return an   |
            ///               | error indicating that a rebalance|
            ///               | is in progress.                  |
            ///               +----------------------------------+
            /// </remarks>
            public static class States
            {
                public const string Dead = "Dead";
                public const string Stable = "Stable";
                public const string AwaitingSync = "AwaitingSync";
                public const string PreparingRebalance = "PreparingRebalance";
                public const string NoActiveGroup = "";
            }

            /// <summary>
            /// The current state of the group (one of: Dead, Stable, AwaitingSync, or PreparingRebalance, or empty if there is no active group)
            /// </summary>
            public string State { get; }

            /// <summary>
            /// The current group protocol type (will be empty if there is no active group)
            /// </summary>
            public string ProtocolType { get; }

            /// <summary>
            /// The current group protocol (only provided if the group is Stable)
            /// </summary>
            public string Protocol { get; }

            /// <summary>
            /// Current group members (only provided if the group is not Dead)
            /// </summary>
            public IImmutableList<Member> Members { get; }

            #region Equality

            /// <inheritdoc />
            public override bool Equals(object obj)
            {
                return Equals(obj as Group);
            }

            /// <inheritdoc />
            public bool Equals(Group other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return ErrorCode == other.ErrorCode 
                       && String.Equals(GroupId, other.GroupId) 
                       && String.Equals(State, other.State) 
                       && String.Equals(ProtocolType, other.ProtocolType) 
                       && String.Equals(Protocol, other.Protocol) 
                       && Members.HasEqualElementsInOrder(other.Members);
            }

            /// <inheritdoc />
            public override int GetHashCode()
            {
                unchecked {
                    var hashCode = (int) ErrorCode;
                    hashCode = (hashCode*397) ^ (GroupId?.GetHashCode() ?? 0);
                    hashCode = (hashCode*397) ^ (State?.GetHashCode() ?? 0);
                    hashCode = (hashCode*397) ^ (ProtocolType?.GetHashCode() ?? 0);
                    hashCode = (hashCode*397) ^ (Protocol?.GetHashCode() ?? 0);
                    hashCode = (hashCode*397) ^ (Members?.Count.GetHashCode() ?? 0);
                    return hashCode;
                }
            }

            /// <inheritdoc />
            public static bool operator ==(Group left, Group right)
            {
                return Equals(left, right);
            }

            /// <inheritdoc />
            public static bool operator !=(Group left, Group right)
            {
                return !Equals(left, right);
            }

            #endregion
        }

        public class Member : IEquatable<Member>
        {
            public override string ToString() => $"{{MemberId:{MemberId},ClientId:{ClientId},ClientHost:{ClientHost},MemberMetadata:{MemberMetadata},MemberAssignment:{MemberAssignment}}}";

            public Member(string memberId, string clientId, string clientHost, IMemberMetadata memberMetadata, IMemberAssignment memberAssignment)
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

            #region Equality

            /// <inheritdoc />
            public override bool Equals(object obj)
            {
                return Equals(obj as Member);
            }

            /// <inheritdoc />
            public bool Equals(Member other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return string.Equals(MemberId, other.MemberId) 
                    && string.Equals(ClientId, other.ClientId) 
                    && string.Equals(ClientHost, other.ClientHost) 
                    && Equals(MemberMetadata, other.MemberMetadata) 
                    && Equals(MemberAssignment, other.MemberAssignment);
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
            public static bool operator ==(Member left, Member right)
            {
                return Equals(left, right);
            }

            /// <inheritdoc />
            public static bool operator !=(Member left, Member right)
            {
                return !Equals(left, right);
            }

            #endregion
        }
    }
}