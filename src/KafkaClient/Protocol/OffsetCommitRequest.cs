using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// Class that represents the api call to commit a specific set of offsets for a given topic.  The offset is saved under the
    /// arbitrary ConsumerGroup name provided by the call.
    /// </summary>
    public class OffsetCommitRequest : Request, IRequest<OffsetCommitResponse>, IEquatable<OffsetCommitRequest>
    {
        public OffsetCommitRequest(string consumerGroup, IEnumerable<OffsetCommit> offsetCommits, string memberId = null, int generationId = 0, TimeSpan? offsetRetention = null) 
            : base(ApiKeyRequestType.OffsetCommit)
        {
            GenerationId = generationId;
            MemberId = memberId;
            ConsumerGroup = consumerGroup;
            OffsetRetention = offsetRetention;
            OffsetCommits = ImmutableList<OffsetCommit>.Empty.AddNotNullRange(offsetCommits);
        }

        /// <summary>
        /// The generation of the consumer group.
        /// Only version 1 (0.8.2) and above
        /// </summary>
        public int GenerationId { get; }

        /// <summary>
        /// The consumer id assigned by the group coordinator.
        /// Only version 1 (0.8.2) and above
        /// </summary>
        public string MemberId { get; }

        /// <summary>
        /// The consumer group id.
        /// </summary>
        public string ConsumerGroup { get; }

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
            return GenerationId == other.GenerationId 
                && string.Equals(MemberId, other.MemberId) 
                && string.Equals(ConsumerGroup, other.ConsumerGroup) 
                && OffsetRetention.Equals(other.OffsetRetention) 
                && OffsetCommits.HasEqualElementsInOrder(other.OffsetCommits);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = GenerationId;
                hashCode = (hashCode*397) ^ (MemberId?.GetHashCode() ?? 0);
                hashCode = (hashCode*397) ^ (ConsumerGroup?.GetHashCode() ?? 0);
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