using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// OffsetCommitRequest => ConsumerGroup *ConsumerGroupGenerationId *MemberId *RetentionTime [TopicName [Partition Offset *TimeStamp Metadata]]
    /// *ConsumerGroupGenerationId, MemberId is only version 1 (0.8.2) and above
    /// *TimeStamp is only version 1 (0.8.2)
    /// *RetentionTime is only version 2 (0.9.0) and above
    ///  ConsumerGroupId => string          -- The consumer group id.
    ///  ConsumerGroupGenerationId => int32 -- The generation of the consumer group.
    ///  MemberId => string                 -- The consumer id assigned by the group coordinator.
    ///  RetentionTime => int64             -- Time period in ms to retain the offset.
    ///  TopicName => string                -- The topic to commit.
    ///  Partition => int32                 -- The partition id.
    ///  Offset => int64                    -- message offset to be committed.
    ///  Timestamp => int64                 -- Commit timestamp.
    ///  Metadata => string                 -- Any associated metadata the client wants to keep
    ///
    /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
    /// Class that represents the api call to commit a specific set of offsets for a given topic.  The offset is saved under the
    /// arbitrary ConsumerGroup name provided by the call.
    /// </summary>
    public class OffsetCommitRequest : GroupRequest, IRequest<OffsetCommitResponse>, IEquatable<OffsetCommitRequest>
    {
        public OffsetCommitRequest(string groupId, IEnumerable<OffsetCommit> offsetCommits, string memberId = null, int generationId = -1, TimeSpan? offsetRetention = null) 
            : base(ApiKeyRequestType.OffsetCommit, groupId, memberId ?? "", generationId)
        {
            OffsetRetention = offsetRetention;
            OffsetCommits = ImmutableList<OffsetCommit>.Empty.AddNotNullRange(offsetCommits);
        }

        /// <summary>
        /// Time period to retain the offset.
        /// </summary>
        public TimeSpan? OffsetRetention { get; }

        public IImmutableList<OffsetCommit> OffsetCommits { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as OffsetCommitRequest);
        }

        /// <inheritdoc />
        public bool Equals(OffsetCommitRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                && OffsetRetention.Equals(other.OffsetRetention) 
                && OffsetCommits.HasEqualElementsInOrder(other.OffsetCommits);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                int hashCode = base.GetHashCode();
                hashCode = (hashCode*397) ^ OffsetRetention.GetHashCode();
                hashCode = (hashCode*397) ^ (OffsetCommits?.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        /// <inheritdoc />
        public static bool operator ==(OffsetCommitRequest left, OffsetCommitRequest right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(OffsetCommitRequest left, OffsetCommitRequest right)
        {
            return !Equals(left, right);
        }
    }
}