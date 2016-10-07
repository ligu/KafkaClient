using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    public class OffsetResponse : IResponse, IEquatable<OffsetResponse>
    {
        public OffsetResponse(OffsetTopic topic)
            : this(new[] {topic})
        {
        }

        public OffsetResponse(IEnumerable<OffsetTopic> topics = null)
        {
            Topics = ImmutableList<OffsetTopic>.Empty.AddNotNullRange(topics);
            Errors = ImmutableList<ErrorResponseCode>.Empty.AddRange(Topics.Select(t => t.ErrorCode));
        }

        public IImmutableList<ErrorResponseCode> Errors { get; }

        public IImmutableList<OffsetTopic> Topics { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as OffsetResponse);
        }

        /// <inheritdoc />
        public bool Equals(OffsetResponse other)
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
        public static bool operator ==(OffsetResponse left, OffsetResponse right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(OffsetResponse left, OffsetResponse right)
        {
            return !Equals(left, right);
        }
    }
}