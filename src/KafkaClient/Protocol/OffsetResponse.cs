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
        public override string ToString() => $"{{Topics:[{Topics.ToStrings()}]}}";

        public OffsetResponse(Topic topic)
            : this(new[] {topic})
        {
        }

        public OffsetResponse(IEnumerable<Topic> topics = null)
        {
            Topics = ImmutableList<Topic>.Empty.AddNotNullRange(topics);
            Errors = ImmutableList<ErrorCode>.Empty.AddRange(Topics.Select(t => t.ErrorCode));
        }

        public IImmutableList<ErrorCode> Errors { get; }

        public IImmutableList<Topic> Topics { get; }

        #region Equality

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
            return Topics?.Count.GetHashCode() ?? 0;
        }

        #endregion

        public class Topic : TopicResponse, IEquatable<Topic>
        {
            public override string ToString() => $"{{TopicName:{TopicName},PartitionId:{PartitionId},ErrorCode:{ErrorCode},Offset:{Offset}}}";

            public Topic(string topic, int partitionId, ErrorCode errorCode = ErrorCode.None, long offset = -1, DateTimeOffset? timestamp = null) 
                : base(topic, partitionId, errorCode)
            {
                Offset = offset;
                Timestamp = timestamp;
            }

            /// <summary>
            /// The timestamp associated with the returned offset.
            /// This only applies to version 1 and above.
            /// </summary>
            public DateTimeOffset? Timestamp { get; set; }

            /// <summary>
            /// The offset found.
            /// </summary>
            public long Offset { get; set; }

            #region Equality

            public override bool Equals(object obj)
            {
                return Equals(obj as Topic);
            }

            public bool Equals(Topic other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return base.Equals(other) 
                    && Timestamp?.ToUnixTimeMilliseconds() == other.Timestamp?.ToUnixTimeMilliseconds()
                    && Offset == other.Offset;
            }

            public override int GetHashCode()
            {
                unchecked {
                    var hashCode = base.GetHashCode();
                    hashCode = (hashCode*397) ^ (Timestamp?.GetHashCode() ?? 0);
                    hashCode = (hashCode*397) ^ Offset.GetHashCode();
                    return hashCode;
                }
            }
        
            #endregion
        }
    }
}