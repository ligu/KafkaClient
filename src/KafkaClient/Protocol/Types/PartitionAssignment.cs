using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol.Types
{
    public class PartitionAssignment
    {
        public PartitionAssignment(string topicName, IEnumerable<int> partitionIds)
        {
            TopicName = topicName;
            PartitionIds = ImmutableList<int>.Empty.AddNotNullRange(partitionIds);
        }

        public string TopicName { get; }
        public IImmutableList<int> PartitionIds { get; }
    }
}