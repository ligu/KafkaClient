using System;
using KafkaClient.Common;

namespace KafkaClient.Protocol.Types
{
    public class ByteMemberMetadata : IMemberMetadata, IEquatable<ByteMemberMetadata>
    {
        private static readonly byte[] Empty = {};

        public ByteMemberMetadata(string protocolType, byte[] bytes)
        {
            Bytes = bytes ?? Empty;
            ProtocolType = protocolType;
        }

        public string ProtocolType { get; }

        public byte[] Bytes { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as ByteMemberMetadata);
        }

        public override int GetHashCode()
        {
            unchecked {
                return ((ProtocolType?.GetHashCode() ?? 0) * 397) ^ (Bytes?.GetHashCode() ?? 0);
            }
        }

        public static bool operator ==(ByteMemberMetadata left, ByteMemberMetadata right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(ByteMemberMetadata left, ByteMemberMetadata right)
        {
            return !Equals(left, right);
        }

        /// <inheritdoc />
        public bool Equals(ByteMemberMetadata other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Equals(ProtocolType, other.ProtocolType) 
                && Bytes.HasEqualElementsInOrder(other.Bytes);
        }

    }
}