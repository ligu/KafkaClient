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
        public override string ToString() => $"{{ErrorCode:{ErrorCode},Groups:[{Groups.ToStrings()}]}}";

        public ListGroupsResponse(ErrorCode errorCode = ErrorCode.None, IEnumerable<Group> groups = null)
        {
            ErrorCode = errorCode;
            Errors = ImmutableList<ErrorCode>.Empty.Add(ErrorCode);
            Groups = ImmutableList<Group>.Empty.AddNotNullRange(groups);
        }

        public IImmutableList<ErrorCode> Errors { get; }

        /// <summary>
        /// The error code.
        /// </summary>
        public ErrorCode ErrorCode { get; }

        public IImmutableList<Group> Groups { get; }

        #region Equality

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
                return ((int) ErrorCode*397) ^ (Groups?.Count.GetHashCode() ?? 0);
            }
        }

        #endregion

        public class Group : IEquatable<Group>
        {
            public override string ToString() => $"{{GroupId:{GroupId},ProtocolType:{ProtocolType}}}";

            public Group(string groupId, string protocolType)
            {
                GroupId = groupId;
                ProtocolType = protocolType;
            }

            public string GroupId { get; }
            public string ProtocolType { get; }

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
                return string.Equals(GroupId, other.GroupId) 
                       && string.Equals(ProtocolType, other.ProtocolType);
            }

            /// <inheritdoc />
            public override int GetHashCode()
            {
                unchecked {
                    return ((GroupId?.GetHashCode() ?? 0)*397) ^ (ProtocolType?.GetHashCode() ?? 0);
                }
            }
            
            #endregion
        }
    }
}