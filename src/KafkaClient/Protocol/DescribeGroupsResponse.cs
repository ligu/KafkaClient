using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// DescribeGroupsResponse => [ErrorCode GroupId State ProtocolType Protocol Members]
    ///   ErrorCode => int16
    ///   GroupId => string
    ///   State => string
    ///   ProtocolType => string
    ///   Protocol => string
    ///   Members => [MemberId ClientId ClientHost MemberMetadata MemberAssignment]
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
        public DescribeGroupsResponse(IEnumerable<DescribeGroup> groups)
        {
            Groups = ImmutableList<DescribeGroup>.Empty.AddNotNullRange(groups);
            Errors = ImmutableList<ErrorResponseCode>.Empty.AddRange(Groups.Select(g => g.ErrorCode));
        }

        public IImmutableList<ErrorResponseCode> Errors { get; }

        public IImmutableList<DescribeGroup> Groups { get; }

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
            return Groups?.GetHashCode() ?? 0;
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
    }
}