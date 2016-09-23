using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    public class OffsetCommitResponse : IResponse
    {
        public OffsetCommitResponse(IEnumerable<TopicResponse> topics = null)
        {
            Topics = ImmutableList<TopicResponse>.Empty.AddNotNullRange(topics);
            Errors = ImmutableList<ErrorResponseCode>.Empty.AddRange(Topics.Select(t => t.ErrorCode));
        }

        public ImmutableList<ErrorResponseCode> Errors { get; }

        public ImmutableList<TopicResponse> Topics { get; }
    }
}