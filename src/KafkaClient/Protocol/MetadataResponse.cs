using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// MetadataResponse => [Broker][TopicMetadata]
    ///  Broker => NodeId Host Port  (any number of brokers may be returned)
    ///                               -- The node id, hostname, and port information for a kafka broker
    ///   NodeId => int32             -- The broker id.
    ///   Host => string              -- The hostname of the broker.
    ///   Port => int32               -- The port on which the broker accepts requests.
    ///  TopicMetadata => TopicErrorCode TopicName [PartitionMetadata]
    ///   TopicErrorCode => int16     -- The error code for the given topic.
    ///   TopicName => string         -- The name of the topic.
    ///  PartitionMetadata => PartitionErrorCode PartitionId Leader Replicas Isr
    ///   PartitionErrorCode => int16 -- The error code for the partition, if any.
    ///   PartitionId => int32        -- The id of the partition.
    ///   Leader => int32             -- The id of the broker acting as leader for this partition.
    ///                                  If no leader exists because we are in the middle of a leader election this id will be -1.
    ///   Replicas => [int32]         -- The set of all nodes that host this partition.
    ///   Isr => [int32]              -- The set of nodes that are in sync with the leader for this partition.
    ///
    /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-MetadataAPI
    /// </summary>
    public class MetadataResponse : IResponse, IEquatable<MetadataResponse>
    {
        public MetadataResponse(IEnumerable<Broker> brokers = null, IEnumerable<MetadataTopic> topics = null)
        {
            Brokers = ImmutableList<Broker>.Empty.AddNotNullRange(brokers);
            Topics = ImmutableList<MetadataTopic>.Empty.AddNotNullRange(topics);
            Errors = ImmutableList<ErrorResponseCode>.Empty.AddRange(Topics.Select(t => t.ErrorCode));
        }

        public IImmutableList<ErrorResponseCode> Errors { get; }

        public IImmutableList<Broker> Brokers { get; }
        public IImmutableList<MetadataTopic> Topics { get; }

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
                && Topics.HasEqualElementsInOrder(other.Topics);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                return ((Brokers?.GetHashCode() ?? 0)*397) ^ (Topics?.GetHashCode() ?? 0);
            }
        }

        /// <inheritdoc />
        public static bool operator ==(MetadataResponse left, MetadataResponse right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(MetadataResponse left, MetadataResponse right)
        {
            return !Equals(left, right);
        }
    }
}