using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    public class OffsetFetchResponse : IResponse
    {
        public OffsetFetchResponse(IEnumerable<OffsetFetchTopic> topics = null)
        {
            Topics = ImmutableList<OffsetFetchTopic>.Empty.AddNotNullRange(topics);
            Errors = ImmutableList<ErrorResponseCode>.Empty.AddRange(Topics.Select(t => t.ErrorCode));
        }

        public ImmutableList<ErrorResponseCode> Errors { get; }

        public ImmutableList<OffsetFetchTopic> Topics { get; }
    }
}