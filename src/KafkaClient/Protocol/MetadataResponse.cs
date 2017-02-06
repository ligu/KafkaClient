using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// MetadataResponse => [Broker] *ClusterId *ControllerId [TopicMetadata]
    ///  *ControllerId is only version 1 (0.10.0) and above
    ///  *ClusterId is only version 2 (0.10.1) and above
    /// 
    ///  Broker => NodeId Host Port *Rack  (any number of brokers may be returned)
    ///   *Rack is only version 1 (0.10.0) and above
    ///                                -- The node id, hostname, and port information for a kafka broker
    ///   NodeId => int32              -- The broker id.
    ///   Host => string               -- The hostname of the broker.
    ///   Port => int32                -- The port on which the broker accepts requests.
    ///   Rack => string               -- The rack of the broker.
    ///  ClusterId => string           -- The cluster id that this broker belongs to.
    ///  ControllerId => int32         -- The broker id of the controller broker
    /// 
    ///  TopicMetadata => TopicErrorCode TopicName *IsInternal [PartitionMetadata]
    ///   *IsInternal is only version 1 (0.10.0) and above
    ///   TopicErrorCode => int16      -- The error code for the given topic.
    ///   TopicName => string          -- The name of the topic.
    ///   IsInternal => boolean        -- Indicates if the topic is considered a Kafka internal topic
    /// 
    ///   PartitionMetadata => PartitionErrorCode PartitionId Leader Replicas Isr
    ///    PartitionErrorCode => int16 -- The error code for the partition, if any.
    ///    PartitionId => int32        -- The id of the partition.
    ///    Leader => int32             -- The id of the broker acting as leader for this partition.
    ///                                   If no leader exists because we are in the middle of a leader election this id will be -1.
    ///    Replicas => [int32]         -- The set of all nodes that host this partition.
    ///    Isr => [int32]              -- The set of nodes that are in sync with the leader for this partition.
    ///
    /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-MetadataAPI
    /// </summary>
    public class MetadataResponse : IResponse, IEquatable<MetadataResponse>
    {
        public override string ToString() => $"{{Servers:[{Brokers.ToStrings()}],Topics:[{Topics.ToStrings()}],ClusterId:{ClusterId},ControllerId:{ControllerId}}}";

        public MetadataResponse(IEnumerable<Server> brokers = null, IEnumerable<Topic> topics = null, int? controllerId = null, string clusterId = null)
        {
            Brokers = ImmutableList<Server>.Empty.AddNotNullRange(brokers);
            Topics = ImmutableList<Topic>.Empty.AddNotNullRange(topics);
            ControllerId = controllerId;
            ClusterId = clusterId;
            Errors = ImmutableList<ErrorCode>.Empty.AddRange(Topics.Select(t => t.ErrorCode));
        }

        public IImmutableList<ErrorCode> Errors { get; }

        public IImmutableList<Server> Brokers { get; }
        public int? ControllerId { get; }
        public string ClusterId { get; }
        public IImmutableList<Topic> Topics { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as MetadataResponse);
        }

        /// <inheritdoc />
        public bool Equals(MetadataResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Brokers.HasEqualElementsInOrder(other.Brokers) 
                && ControllerId == other.ControllerId
                && ClusterId == other.ClusterId
                && Topics.HasEqualElementsInOrder(other.Topics);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = Brokers?.Count.GetHashCode() ?? 0;
                hashCode = (hashCode*397) ^ (ControllerId?.GetHashCode() ?? 0);
                hashCode = (hashCode*397) ^ (ClusterId?.GetHashCode() ?? 0);
                hashCode = (hashCode*397) ^ (Topics?.Count.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        #endregion

        public class Topic : IEquatable<Topic>
        {
            public override string ToString() => $"{{TopicName:{TopicName},ErrorCode:{ErrorCode},Partitions:[{Partitions.ToStrings()}],IsInternal:{IsInternal}}}";

            public Topic(string topicName, ErrorCode errorCode = ErrorCode.NONE, IEnumerable<Partition> partitions = null, bool? isInternal = null)
            {
                ErrorCode = errorCode;
                TopicName = topicName;
                IsInternal = isInternal;
                Partitions = ImmutableList<Partition>.Empty.AddNotNullRange(partitions);
            }

            public ErrorCode ErrorCode { get; }

            public string TopicName { get; }
            public bool? IsInternal { get; }

            public IImmutableList<Partition> Partitions { get; }

            #region Equality

            /// <inheritdoc />
            public override bool Equals(object obj)
            {
                return Equals(obj as Topic);
            }

            /// <inheritdoc />
            public bool Equals(Topic other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return ErrorCode == other.ErrorCode 
                    && string.Equals(TopicName, other.TopicName) 
                    && IsInternal == other.IsInternal
                    && Partitions.HasEqualElementsInOrder(other.Partitions);
            }

            /// <inheritdoc />
            public override int GetHashCode()
            {
                unchecked {
                    var hashCode = (int) ErrorCode;
                    hashCode = (hashCode*397) ^ (TopicName?.GetHashCode() ?? 0);
                    hashCode = (hashCode*397) ^ (IsInternal?.GetHashCode() ?? 0);
                    hashCode = (hashCode*397) ^ (Partitions?.GetHashCode() ?? 0);
                    return hashCode;
                }
            }

            #endregion
        }

        public class Partition : IEquatable<Partition>
        {
            public override string ToString() => $"{{PartitionId:{PartitionId},ErrorCode:{ErrorCode},LeaderId:{LeaderId},Replicas:[{Replicas.ToStrings()}],Isr:[{Isrs.ToStrings()}]}}";

            public Partition(int partitionId, int leaderId, ErrorCode errorCode = ErrorCode.NONE, IEnumerable<int> replicas = null, IEnumerable<int> isrs = null)
            {
                ErrorCode = errorCode;
                PartitionId = partitionId;
                LeaderId = leaderId;
                Replicas = ImmutableList<int>.Empty.AddNotNullRange(replicas);
                Isrs = ImmutableList<int>.Empty.AddNotNullRange(isrs);
            }

            /// <summary>
            /// Error code.
            /// </summary>
            public ErrorCode ErrorCode { get; }

            /// <summary>
            /// The Id of the partition that this metadata describes.
            /// </summary>
            public int PartitionId { get; }

            /// <summary>
            /// The node id for the kafka broker currently acting as leader for this partition. If no leader exists because we are in the middle of a leader election this id will be -1.
            /// </summary>
            public int LeaderId { get; }

            public bool IsElectingLeader => LeaderId == -1;

            /// <summary>
            /// The set of alive nodes that currently acts as slaves for the leader for this partition.
            /// </summary>
            public IImmutableList<int> Replicas { get; }

            /// <summary>
            /// The set subset of the replicas that are "caught up" to the leader
            /// </summary>
            public IImmutableList<int> Isrs { get; }

            /// <inheritdoc />
            public override bool Equals(object obj)
            {
                return Equals(obj as Partition);
            }

            /// <inheritdoc />
            public bool Equals(Partition other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return ErrorCode == other.ErrorCode 
                    && PartitionId == other.PartitionId 
                    && LeaderId == other.LeaderId 
                    && Replicas.HasEqualElementsInOrder(other.Replicas) 
                    && Isrs.HasEqualElementsInOrder(other.Isrs);
            }

            /// <inheritdoc />
            public override int GetHashCode()
            {
                unchecked {
                    var hashCode = (int) ErrorCode;
                    hashCode = (hashCode*397) ^ PartitionId;
                    hashCode = (hashCode*397) ^ LeaderId;
                    hashCode = (hashCode*397) ^ (Replicas?.Count.GetHashCode() ?? 0);
                    hashCode = (hashCode*397) ^ (Isrs?.Count.GetHashCode() ?? 0);
                    return hashCode;
                }
            }
        }

    }
}