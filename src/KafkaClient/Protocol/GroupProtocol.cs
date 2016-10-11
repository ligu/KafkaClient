using System;
using KafkaClient.Protocol.Types;

namespace KafkaClient.Protocol
{
    public class GroupProtocol : IEquatable<GroupProtocol>
    {
        public GroupProtocol(string name, IMemberMetadata metadata)
        {
            Name = name;
            Metadata = metadata;
        }

        public string Name { get; }
        public IMemberMetadata Metadata { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as GroupProtocol);
        }

        /// <inheritdoc />
        public bool Equals(GroupProtocol other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return string.Equals(Name, other.Name) 
                && Equals(Metadata, other.Metadata);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                return ((Name?.GetHashCode() ?? 0)*397) ^ (Metadata?.GetHashCode() ?? 0);
            }
        }

        /// <inheritdoc />
        public static bool operator ==(GroupProtocol left, GroupProtocol right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(GroupProtocol left, GroupProtocol right)
        {
            return !Equals(left, right);
        }
    }
}