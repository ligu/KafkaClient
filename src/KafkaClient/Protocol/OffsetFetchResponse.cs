using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    public class OffsetFetchResponse : IResponse, IEquatable<OffsetFetchResponse>
    {
        public OffsetFetchResponse(IEnumerable<OffsetFetchTopic> topics = null)
        {
            Topics = ImmutableList<OffsetFetchTopic>.Empty.AddNotNullRange(topics);
            Errors = ImmutableList<ErrorResponseCode>.Empty.AddRange(Topics.Select(t => t.ErrorCode));
        }

        public IImmutableList<ErrorResponseCode> Errors { get; }

        public IImmutableList<OffsetFetchTopic> Topics { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as OffsetFetchResponse);
        }

        /// <inheritdoc />
        public bool Equals(OffsetFetchResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Topics.HasEqualElementsInOrder(other.Topics);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return Topics?.GetHashCode() ?? 0;
        }

        /// <inheritdoc />
        public static bool operator ==(OffsetFetchResponse left, OffsetFetchResponse right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(OffsetFetchResponse left, OffsetFetchResponse right)
        {
            return !Equals(left, right);
        }
    }
}