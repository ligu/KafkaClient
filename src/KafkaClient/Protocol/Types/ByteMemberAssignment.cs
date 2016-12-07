using System;
using KafkaClient.Common;

namespace KafkaClient.Protocol.Types
{
    public class ByteMemberAssignment : IMemberAssignment, IEquatable<ByteMemberAssignment>
    {
        private static readonly byte[] Empty = {};

        public ByteMemberAssignment(byte[] bytes)
        {
            Bytes = bytes ?? Empty;
        }

        public byte[] Bytes { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as ByteMemberAssignment);
        }

        public override int GetHashCode()
        {
            return Bytes?.GetHashCode() ?? 0;
        }


        public static bool operator ==(ByteMemberAssignment left, ByteMemberAssignment right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(ByteMemberAssignment left, ByteMemberAssignment right)
        {
            return !Equals(left, right);
        }

        /// <inheritdoc />
        public bool Equals(ByteMemberAssignment other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Bytes.HasEqualElementsInOrder(other.Bytes);
        }

    }
}