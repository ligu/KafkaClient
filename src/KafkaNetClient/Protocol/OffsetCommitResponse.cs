using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace KafkaNet.Protocol
{
    public class OffsetCommitResponse : IKafkaResponse
    {
        public OffsetCommitResponse(IEnumerable<TopicResponse> topics = null)
        {
            Topics = topics != null ? ImmutableList<TopicResponse>.Empty.AddRange(topics) : ImmutableList<TopicResponse>.Empty;
            Errors = ImmutableList<ErrorResponseCode>.Empty.AddRange(Topics.Select(t => t.ErrorCode));
        }

        public ImmutableList<ErrorResponseCode> Errors { get; }

        public ImmutableList<TopicResponse> Topics { get; }
    }
}