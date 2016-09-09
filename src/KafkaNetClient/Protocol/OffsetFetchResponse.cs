using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace KafkaNet.Protocol
{
    public class OffsetFetchResponse : IKafkaResponse
    {
        public OffsetFetchResponse(IEnumerable<OffsetFetchTopic> topics = null)
        {
            Topics = topics != null ? ImmutableList<OffsetFetchTopic>.Empty.AddRange(topics) : ImmutableList<OffsetFetchTopic>.Empty;
            Errors = ImmutableList<ErrorResponseCode>.Empty.AddRange(Topics.Select(t => t.ErrorCode));
        }

        public ImmutableList<ErrorResponseCode> Errors { get; }

        public ImmutableList<OffsetFetchTopic> Topics { get; }
    }
}