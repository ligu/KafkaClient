using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace KafkaClient.Protocol
{
    public class FetchResponse : IKafkaResponse
    {
        public FetchResponse(IEnumerable<FetchTopicResponse> topics = null, TimeSpan? throttleTime = null)
        {
            Topics = topics != null ? ImmutableList<FetchTopicResponse>.Empty.AddRange(topics) : ImmutableList<FetchTopicResponse>.Empty;
            Errors = ImmutableList<ErrorResponseCode>.Empty.AddRange(Topics.Select(t => t.ErrorCode));
            ThrottleTime = throttleTime;
        }

        public ImmutableList<ErrorResponseCode> Errors { get; }

        public ImmutableList<FetchTopicResponse> Topics { get; }

        /// <summary>
        /// Duration in milliseconds for which the request was throttled due to quota violation. (Zero if the request did not 
        /// violate any quota.) Only version 1 and above (0.9.0)
        /// </summary>
        public TimeSpan? ThrottleTime { get; }
    }
}