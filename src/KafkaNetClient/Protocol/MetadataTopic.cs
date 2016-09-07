using System.Collections.Generic;
using System.Collections.Immutable;

namespace KafkaNet.Protocol
{
    public class MetadataTopic
    {
        public MetadataTopic(string name, ErrorResponseCode errorCode = ErrorResponseCode.NoError, IEnumerable<MetadataPartition> partitions = null)
        {
            ErrorCode = errorCode;
            Name = name;
            Partitions = partitions != null ? ImmutableList<MetadataPartition>.Empty.AddRange(partitions) : ImmutableList<MetadataPartition>.Empty;
        }

        public ErrorResponseCode ErrorCode { get; }

        public string Name { get; }

        public ImmutableList<MetadataPartition> Partitions { get; }
    }
}