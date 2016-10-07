using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    public class ProduceResponse : IResponse, IEquatable<ProduceResponse>
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

        public IImmutableList<ErrorResponseCode> Errors { get; }

        public IImmutableList<ProduceTopic> Topics { get; }

        /// <summary>
        /// Duration in milliseconds for which the request was throttled due to quota violation. 
        /// (Zero if the request did not violate any quota). Only version 1 (0.9.0) and above.
        /// </summary>
        public TimeSpan? ThrottleTime { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as ProduceResponse);
        }

        /// <inheritdoc />
        public bool Equals(ProduceResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Topics.HasEqualElementsInOrder(other.Topics) 
                && (int?)ThrottleTime?.TotalMilliseconds == (int?)other.ThrottleTime?.TotalMilliseconds;
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = Topics?.GetHashCode() ?? 0;
                hashCode = (hashCode*397) ^ ThrottleTime.GetHashCode();
                return hashCode;
            }
        }

        /// <inheritdoc />
        public static bool operator ==(ProduceResponse left, ProduceResponse right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(ProduceResponse left, ProduceResponse right)
        {
            return !Equals(left, right);
        }
    }
}