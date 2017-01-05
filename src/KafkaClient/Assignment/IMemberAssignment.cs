using System.Collections.Immutable;
using KafkaClient.Protocol;

namespace KafkaClient.Assignment
{
    public interface IMemberAssignment
    {
        IImmutableList<TopicPartition> PartitionAssignments { get; }
    }
}