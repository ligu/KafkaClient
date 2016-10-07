using System;

namespace KafkaClient.Protocol
{
    public class ListGroup : IEquatable<ListGroup>
    {
        public ListGroup(string groupId, string protocolType)
        {
            GroupId = groupId;
            ProtocolType = protocolType;
        }

        public string GroupId { get; }
        public string ProtocolType { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as ListGroup);
        }

        /// <inheritdoc />
        public bool Equals(ListGroup other)
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

        /// <inheritdoc />
        public static bool operator ==(ListGroup left, ListGroup right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(ListGroup left, ListGroup right)
        {
            return !Equals(left, right);
        }
    }
}