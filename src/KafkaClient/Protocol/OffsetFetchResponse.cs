using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// OffsetFetchResponse => [TopicName [Partition Offset Metadata ErrorCode]]
    ///  TopicName => string -- The name of the topic.
    ///  Partition => int32  -- The id of the partition.
    ///  Offset => int64     -- The offset, or -1 if none exists.
    ///  Metadata => string  -- The metadata associated with the topic and partition.
    ///  ErrorCode => int16  -- The error code for the partition, if any.
    ///
    /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
    /// </summary>
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