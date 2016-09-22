using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    public class OffsetResponse : IKafkaResponse
    {
        public OffsetResponse(OffsetTopic topic)
            : this(new[] {topic})
        {
        }

        public OffsetResponse(IEnumerable<OffsetTopic> topics = null)
        {
            Topics = ImmutableList<OffsetTopic>.Empty.AddNotNullRange(topics);
            Errors = ImmutableList<ErrorResponseCode>.Empty.AddRange(Topics.Select(t => t.ErrorCode));
        }

        public ImmutableList<ErrorResponseCode> Errors { get; }

        public ImmutableList<OffsetTopic> Topics { get; }
    }
}