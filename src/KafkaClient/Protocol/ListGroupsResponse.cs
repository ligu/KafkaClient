using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// ListGroupsResponse => ErrorCode Groups
    ///   ErrorCode => int16
    ///   Groups => [GroupId ProtocolType]
    ///     GroupId => string
    ///     ProtocolType => string
    ///
    /// From http://kafka.apache.org/protocol.html#protocol_messages
    /// </summary>
    public class ListGroupsResponse : IResponse, IEquatable<ListGroupsResponse>
    {
        public ListGroupsResponse(ErrorResponseCode errorCode = ErrorResponseCode.None, IEnumerable<ListGroup> groups = null)
        {
            ErrorCode = errorCode;
            Errors = ImmutableList<ErrorResponseCode>.Empty.Add(ErrorCode);
            Groups = ImmutableList<ListGroup>.Empty.AddNotNullRange(groups);
        }

        public IImmutableList<ErrorResponseCode> Errors { get; }

        /// <summary>
        /// The error code.
        /// </summary>
        public ErrorResponseCode ErrorCode { get; }

        public IImmutableList<ListGroup> Groups { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as ListGroupsResponse);
        }

        /// <inheritdoc />
        public bool Equals(ListGroupsResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return ErrorCode == other.ErrorCode
                   && Groups.HasEqualElementsInOrder(other.Groups);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                return ((int) ErrorCode*397) ^ (Groups?.GetHashCode() ?? 0);
            }
        }

        /// <inheritdoc />
        public static bool operator ==(ListGroupsResponse left, ListGroupsResponse right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(ListGroupsResponse left, ListGroupsResponse right)
        {
            return !Equals(left, right);
        }
    }
}