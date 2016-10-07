using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    public class DescribeGroup : IEquatable<DescribeGroup>
    {
        public DescribeGroup(ErrorResponseCode errorCode, string groupId, string state, string protocolType, string protocol, IEnumerable<DescribeGroupMember> members)
        {
            ErrorCode = errorCode;
            GroupId = groupId;
            State = state;
            ProtocolType = protocolType;
            Protocol = protocol;
            Members = ImmutableList<DescribeGroupMember>.Empty.AddNotNullRange(members);
        }

        public ErrorResponseCode ErrorCode { get; }
        public string GroupId { get; }

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
        public IImmutableList<DescribeGroupMember> Members { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as DescribeGroup);
        }

        /// <inheritdoc />
        public bool Equals(DescribeGroup other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return ErrorCode == other.ErrorCode 
                   && string.Equals(GroupId, other.GroupId) 
                   && string.Equals(State, other.State) 
                   && string.Equals(ProtocolType, other.ProtocolType) 
                   && string.Equals(Protocol, other.Protocol) 
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
                hashCode = (hashCode*397) ^ (Members?.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        /// <inheritdoc />
        public static bool operator ==(DescribeGroup left, DescribeGroup right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(DescribeGroup left, DescribeGroup right)
        {
            return !Equals(left, right);
        }
    }
}