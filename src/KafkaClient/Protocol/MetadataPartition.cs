using System.Collections.Generic;
using System.Collections.Immutable;

namespace KafkaClient.Protocol
{
    public class MetadataPartition
    {
        public MetadataPartition(int partitionId, int leaderId, ErrorResponseCode errorCode = ErrorResponseCode.NoError, IEnumerable<int> replicas = null, IEnumerable<int> isrs = null)
        {
            ErrorCode = errorCode;
            PartitionId = partitionId;
            LeaderId = leaderId;
            Replicas = replicas != null ? ImmutableList<int>.Empty.AddRange(replicas) : ImmutableList<int>.Empty;
            Isrs = isrs != null ? ImmutableList<int>.Empty.AddRange(isrs) : ImmutableList<int>.Empty;
        }

        /// <summary>
        /// Error code.
        /// </summary>
        public ErrorResponseCode ErrorCode { get; }

        /// <summary>
        /// The Id of the partition that this metadata describes.
        /// </summary>
        public int PartitionId { get; }

        /// <summary>
        /// The node id for the kafka broker currently acting as leader for this partition. If no leader exists because we are in the middle of a leader election this id will be -1.
        /// </summary>
        public int LeaderId { get; }

        /// <summary>
        /// The set of alive nodes that currently acts as slaves for the leader for this partition.
        /// </summary>
        public ImmutableList<int> Replicas { get; }

        /// <summary>
        /// The set subset of the replicas that are "caught up" to the leader
        /// </summary>
        public ImmutableList<int> Isrs { get; }

        protected bool Equals(MetadataPartition other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return PartitionId == other.PartitionId;
        }

        public override int GetHashCode()
        {
            return PartitionId;
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as MetadataPartition);
        }
    }
}