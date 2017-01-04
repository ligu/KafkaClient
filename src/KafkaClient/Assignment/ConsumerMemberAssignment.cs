using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;
using KafkaClient.Protocol;

namespace KafkaClient.Assignment
{
    /// <summary>
    ///  The format of the MemberAssignment field for consumer groups is included below:
    /// MemberAssignment => Version PartitionAssignment
    ///   Version => int16
    ///   PartitionAssignment => [Topic [Partition]]
    ///     Topic => string
    ///     Partition => int32
    ///   UserData => bytes
    /// All client implementations using the "consumer" protocol type should support this schema.
    /// 
    /// from https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
    /// </summary>
    public class ConsumerMemberAssignment : IMemberAssignment, IEquatable<ConsumerMemberAssignment>
    {
        public ConsumerMemberAssignment(short version = 0, IEnumerable<TopicPartition> partitionAssignments = null)
        {
            Version = version;
            PartitionAssignments = ImmutableList<TopicPartition>.Empty.AddNotNullRange(partitionAssignments);
        }

        public short Version { get; }
        public IImmutableList<TopicPartition> PartitionAssignments { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as ConsumerMemberAssignment);
        }

        /// <inheritdoc />
        public bool Equals(ConsumerMemberAssignment other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Version == other.Version 
                && PartitionAssignments.HasEqualElementsInOrder(other.PartitionAssignments);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                return (Version.GetHashCode()*397) ^ (PartitionAssignments?.GetHashCode() ?? 0);
            }
        }

        /// <inheritdoc />
        public static bool operator ==(ConsumerMemberAssignment left, ConsumerMemberAssignment right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(ConsumerMemberAssignment left, ConsumerMemberAssignment right)
        {
            return !Equals(left, right);
        }
    }
}