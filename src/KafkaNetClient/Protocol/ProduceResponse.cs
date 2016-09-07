using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.Contracts;
using System.Linq;

namespace KafkaNet.Protocol
{
    public class ProduceResponse : IKafkaResponse
    {
        public ProduceResponse(int correlationId, IEnumerable<ProduceTopic> topics = null, TimeSpan? throttleTime = null)
        {
            CorrelationId = correlationId;
            Topics = topics != null ? ImmutableList<ProduceTopic>.Empty.AddRange(topics) : ImmutableList<ProduceTopic>.Empty;
            Errors = ImmutableList<ErrorResponseCode>.Empty.AddRange(Topics.Select(t => t.Error));
            ThrottleTime = throttleTime;
        }

        /// <summary>
        /// Request Correlation
        /// </summary>
        public int CorrelationId { get; }

        public ImmutableList<ErrorResponseCode> Errors { get; }

        public ImmutableList<ProduceTopic> Topics { get; }

        /// <summary>
        /// Duration in milliseconds for which the request was throttled due to quota violation. 
        /// (Zero if the request did not violate any quota). Only version 1 (0.9.0) and above.
        /// </summary>
        public TimeSpan? ThrottleTime { get; }
    }
}