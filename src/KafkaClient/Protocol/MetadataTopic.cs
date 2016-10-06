using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    public class MetadataTopic
    {
        public MetadataTopic(string topicName, ErrorResponseCode errorCode = ErrorResponseCode.None, IEnumerable<MetadataPartition> partitions = null)
        {
            ErrorCode = errorCode;
            TopicName = topicName;
            Partitions = ImmutableList<MetadataPartition>.Empty.AddNotNullRange(partitions);
        }

        public ErrorResponseCode ErrorCode { get; }

        public string TopicName { get; }

        public ImmutableList<MetadataPartition> Partitions { get; }
    }
}