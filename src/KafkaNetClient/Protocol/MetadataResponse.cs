using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace KafkaNet.Protocol
{
    public class MetadataResponse : IKafkaResponse
    {
        public MetadataResponse(int correlationId, IEnumerable<Broker> brokers = null, IEnumerable<MetadataTopic> topics = null)
        {
            CorrelationId = correlationId;
            Brokers = brokers != null ? ImmutableList<Broker>.Empty.AddRange(brokers) : ImmutableList<Broker>.Empty;
            Topics = topics != null ? ImmutableList<MetadataTopic>.Empty.AddRange(topics) : ImmutableList<MetadataTopic>.Empty;
            Errors = ImmutableList<ErrorResponseCode>.Empty.AddRange(Topics.Select(t => t.ErrorCode));
        }

        public int CorrelationId { get; }
        public ImmutableList<ErrorResponseCode> Errors { get; }

        public ImmutableList<Broker> Brokers { get; }
        public ImmutableList<MetadataTopic> Topics { get; }
    }
}