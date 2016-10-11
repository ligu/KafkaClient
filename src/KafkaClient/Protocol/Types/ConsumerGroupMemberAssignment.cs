using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol.Types
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
    public class ConsumerGroupMemberAssignment
    {
        public ConsumerGroupMemberAssignment(short version, IEnumerable<PartitionAssignment> partitionAssignments)
        {
            Version = version;
            PartitionAssignments = ImmutableList<PartitionAssignment>.Empty.AddNotNullRange(partitionAssignments);
        }

        public short Version { get; }
        public IImmutableList<PartitionAssignment> PartitionAssignments { get; }
    }
}