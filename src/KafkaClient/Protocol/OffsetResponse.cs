using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace KafkaNet.Protocol
{
    public class OffsetResponse : IKafkaResponse
    {
        public OffsetResponse(OffsetTopic topic)
            : this(new[] {topic})
        {
        }

        public OffsetResponse(IEnumerable<OffsetTopic> topics = null)
        {
            Topics = topics != null ? ImmutableList<OffsetTopic>.Empty.AddRange(topics) : ImmutableList<OffsetTopic>.Empty;
            Errors = ImmutableList<ErrorResponseCode>.Empty.AddRange(Topics.Select(t => t.ErrorCode));
        }

        public ImmutableList<ErrorResponseCode> Errors { get; }

        public ImmutableList<OffsetTopic> Topics { get; }
    }
}