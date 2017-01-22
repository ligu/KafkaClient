using System;
using System.Collections.Immutable;
using KafkaClient.Assignment;
using KafkaClient.Common;
using KafkaClient.Protocol;

namespace KafkaClient.Tests
{
    public class ByteTypeAssignment : IMemberAssignment, IEquatable<ByteTypeAssignment>
    {
        public ByteTypeAssignment(ArraySegment<byte> bytes)
        {
            PartitionAssignments = ImmutableList<TopicPartition>.Empty;
            Bytes = bytes;
        }

        public ArraySegment<byte> Bytes { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as ByteTypeAssignment);
        }

        public override int GetHashCode()
        {
            return Bytes.GetHashCode();
        }


        public static bool operator ==(ByteTypeAssignment left, ByteTypeAssignment right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(ByteTypeAssignment left, ByteTypeAssignment right)
        {
            return !Equals(left, right);
        }

        /// <inheritdoc />
        public bool Equals(ByteTypeAssignment other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Bytes.HasEqualElementsInOrder(other.Bytes);
        }

        public IImmutableList<TopicPartition> PartitionAssignments { get; }
    }
}