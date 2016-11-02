using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// OffsetResponse => [TopicData]
    ///  TopicData => TopicName [PartitionData]
    ///   TopicName => string  -- The name of the topic.
    /// 
    ///   PartitionData => Partition ErrorCode *Timestamp *Offset *[Offset]
    ///    *Timestamp, *Offset only applies to version 1 (Kafka 0.10.1 and higher)
    ///    *[Offset] only applies to version 0 (Kafka 0.10.0.1 and below)
    ///    Partition => int32  -- The id of the partition the fetch is for.
    ///    ErrorCode => int16  -- The error from this partition, if any. Errors are given on a per-partition basis because a given partition may 
    ///                          be unavailable or maintained on a different host, while others may have successfully accepted the produce request.
    ///    Timestamp => int64  -- The timestamp associated with the returned offset
    ///    Offset => int64 -- offset found
    /// 
    /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetAPI(AKAListOffset)
    /// </summary>
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