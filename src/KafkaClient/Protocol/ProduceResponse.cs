using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    public class ProduceResponse : IResponse
    {
        public ProduceResponse(ProduceTopic topic, TimeSpan? throttleTime = null)
            : this (new []{ topic }, throttleTime)
        {
        }

        public ProduceResponse(IEnumerable<ProduceTopic> topics = null, TimeSpan? throttleTime = null)
        {
            Topics = ImmutableList<ProduceTopic>.Empty.AddNotNullRange(topics);
            Errors = ImmutableList<ErrorResponseCode>.Empty.AddRange(Topics.Select(t => t.ErrorCode));
            ThrottleTime = throttleTime;
        }

        public ImmutableList<ErrorResponseCode> Errors { get; }

        public ImmutableList<ProduceTopic> Topics { get; }

        /// <summary>
        /// Duration in milliseconds for which the request was throttled due to quota violation. 
        /// (Zero if the request did not violate any quota). Only version 1 (0.9.0) and above.
        /// </summary>
        public TimeSpan? ThrottleTime { get; }
    }
}