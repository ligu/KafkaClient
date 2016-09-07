using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace KafkaNet.Protocol
{
    public class OffsetCommitResponse : IKafkaResponse
    {
        public OffsetCommitResponse(int correlationId, IEnumerable<TopicResponse> topics = null)
        {
            CorrelationId = correlationId;
            Topics = topics != null ? ImmutableList<TopicResponse>.Empty.AddRange(topics) : ImmutableList<TopicResponse>.Empty;
            Errors = ImmutableList<ErrorResponseCode>.Empty.AddRange(Topics.Select(t => t.Error));
        }

        /// <summary>
        /// Request Correlation
        /// </summary>
        public int CorrelationId { get; }

        public ImmutableList<ErrorResponseCode> Errors { get; }

        public ImmutableList<TopicResponse> Topics { get; }
    }
}