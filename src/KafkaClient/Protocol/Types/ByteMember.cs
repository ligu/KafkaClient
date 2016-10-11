using System;
using KafkaClient.Common;

namespace KafkaClient.Protocol.Types
{
    public class ByteMember : IMemberAssignment, IMemberMetadata, IEquatable<ByteMember>
    {
        private static readonly byte[] Empty = {};

        public ByteMember(byte[] bytes)
        {
            Bytes = bytes ?? Empty;
        }

        public byte[] Bytes { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as ByteMember);
        }

        /// <inheritdoc />
        public bool Equals(ByteMember other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Bytes.HasEqualElementsInOrder(other.Bytes);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return Bytes?.GetHashCode() ?? 0;
        }

        /// <inheritdoc />
        public static bool operator ==(ByteMember left, ByteMember right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(ByteMember left, ByteMember right)
        {
            return !Equals(left, right);
        }
    }
}