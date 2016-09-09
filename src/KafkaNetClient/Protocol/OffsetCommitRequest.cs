using System;
using System.Collections.Generic;
using System.Collections.Immutable;

namespace KafkaNet.Protocol
{
    /// <summary>
    /// Class that represents the api call to commit a specific set of offsets for a given topic.  The offset is saved under the
    /// arbitrary ConsumerGroup name provided by the call.
    /// </summary>
    public class OffsetCommitRequest : KafkaRequest
    {
        public OffsetCommitRequest(string consumerGroup, string memberId, int generationId, IEnumerable<OffsetCommit> offsetCommits, TimeSpan? offsetRetention = null) 
            : base(ApiKeyRequestType.OffsetCommit)
        {
            GenerationId = generationId;
            MemberId = memberId;
            ConsumerGroup = consumerGroup;
            OffsetRetention = offsetRetention;
            OffsetCommits = offsetCommits != null ? ImmutableList<OffsetCommit>.Empty.AddRange(offsetCommits) : ImmutableList<OffsetCommit>.Empty;
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

        public ImmutableList<OffsetCommit> OffsetCommits { get; }
    }
}