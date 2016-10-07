using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    public class FetchResponse : IResponse, IEquatable<FetchResponse>
    {
        public FetchResponse(IEnumerable<FetchTopicResponse> topics = null, TimeSpan? throttleTime = null)
        {
            Topics = ImmutableList<FetchTopicResponse>.Empty.AddNotNullRange(topics);
            Errors = ImmutableList<ErrorResponseCode>.Empty.AddRange(Topics.Select(t => t.ErrorCode));
            ThrottleTime = throttleTime;
        }

        public IImmutableList<ErrorResponseCode> Errors { get; }

        public IImmutableList<FetchTopicResponse> Topics { get; }

        /// <summary>
        /// Duration in milliseconds for which the request was throttled due to quota violation. (Zero if the request did not 
        /// violate any quota.) Only version 1 and above (0.9.0)
        /// </summary>
        public TimeSpan? ThrottleTime { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as FetchResponse);
        }

        /// <inheritdoc />
        public bool Equals(FetchResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Topics.HasEqualElementsInOrder(other.Topics) 
                && ThrottleTime.Equals(other.ThrottleTime);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                return ((Topics?.GetHashCode() ?? 0)*397) ^ ThrottleTime.GetHashCode();
            }
        }

        /// <inheritdoc />
        public static bool operator ==(FetchResponse left, FetchResponse right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(FetchResponse left, FetchResponse right)
        {
            return !Equals(left, right);
        }
    }
}