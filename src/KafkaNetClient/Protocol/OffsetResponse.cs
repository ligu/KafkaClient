using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace KafkaNet.Protocol
{
    public class OffsetResponse : IKafkaResponse
    {
        public OffsetResponse(int correlationId, IEnumerable<OffsetTopic> topics = null)
        {
            CorrelationId = correlationId;
            Topics = topics != null ? ImmutableList<OffsetTopic>.Empty.AddRange(topics) : ImmutableList<OffsetTopic>.Empty;
            Errors = ImmutableList<ErrorResponseCode>.Empty.AddRange(Topics.Select(t => t.ErrorCode));
        }

        /// <inheritdoc />
        public int CorrelationId { get; }

        public ImmutableList<ErrorResponseCode> Errors { get; }

        public ImmutableList<OffsetTopic> Topics { get; }
    }
}