using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace KafkaNet.Protocol
{
    public class MetadataResponse : IKafkaResponse
    {
        public MetadataResponse(int correlationId, IEnumerable<MetadataBroker> brokers = null, IEnumerable<MetadataTopic> topics = null)
        {
            CorrelationId = correlationId;
            Brokers = brokers != null ? ImmutableList<MetadataBroker>.Empty.AddRange(brokers) : ImmutableList<MetadataBroker>.Empty;
            Topics = topics != null ? ImmutableList<MetadataTopic>.Empty.AddRange(topics) : ImmutableList<MetadataTopic>.Empty;
            Errors = ImmutableList<ErrorResponseCode>.Empty.AddRange(Topics.Select(t => t.ErrorCode));
        }

        public int CorrelationId { get; }
        public ImmutableList<ErrorResponseCode> Errors { get; }

        public ImmutableList<MetadataBroker> Brokers { get; }
        public ImmutableList<MetadataTopic> Topics { get; }
    }
}