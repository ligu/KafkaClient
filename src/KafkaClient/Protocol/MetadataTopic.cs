using System.Collections.Generic;
using System.Collections.Immutable;

namespace KafkaNet.Protocol
{
    public class MetadataTopic
    {
        public MetadataTopic(string topicName, ErrorResponseCode errorCode = ErrorResponseCode.NoError, IEnumerable<MetadataPartition> partitions = null)
        {
            ErrorCode = errorCode;
            TopicName = topicName;
            Partitions = partitions != null ? ImmutableList<MetadataPartition>.Empty.AddRange(partitions) : ImmutableList<MetadataPartition>.Empty;
        }

        public ErrorResponseCode ErrorCode { get; }

        public string TopicName { get; }

        public ImmutableList<MetadataPartition> Partitions { get; }
    }
}