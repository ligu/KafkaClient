using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;
// ReSharper disable InconsistentNaming

namespace KafkaClient.Protocol
{
    /// <summary>
    /// Metadata Response => [brokers] *cluster_id *controller_id [topic_metadata]
    ///  *controller_id is only version 1 (0.10.0) and above
    ///  *cluster_id is only version 2 (0.10.1) and above
    /// 
    ///  broker => node_id host port *rack  (any number of brokers may be returned)
    ///   *Rack is only version 1 (0.10.0) and above
    ///                                  -- The node id, hostname, and port information for a kafka broker
    ///   node_id => INT32               -- The broker id.
    ///   host => STRING                 -- The hostname of the broker.
    ///   port => INT32                  -- The port on which the broker accepts requests.
    ///   rack => NULLABLE_STRING        -- The rack of the broker.
    ///  cluster_id => NULLABLE_STRING   -- The cluster id that this broker belongs to.
    ///  controller_id => INT32          -- The broker id of the controller broker
    /// 
    ///  topic_metadata => topic_error_code topic *is_internal [partition_metadata]
    ///   *is_internal is only version 1 (0.10.0) and above
    ///   topic_error_code => INT16      -- The error code for the given topic.
    ///   topic => STRING                -- The name of the topic.
    ///   is_internal => BOOLEAN         -- Indicates if the topic is considered a Kafka internal topic
    /// 
    ///   partition_metadata => partition_error_code partition_id leader replicas isr
    ///    partition_error_code => INT16 -- The error code for the partition, if any.
    ///    partition_id => INT32         -- The id of the partition.
    ///    leader => INT32               -- The id of the broker acting as leader for this partition.
    ///                                     If no leader exists because we are in the middle of a leader election this id will be -1.
    ///    replicas => [INT32]           -- The set of all nodes that host this partition.
    ///    isr => [INT32]                -- The set of nodes that are in sync with the leader for this partition.
    ///
    /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-MetadataAPI
    /// </summary>
    public class MetadataResponse : IResponse, IEquatable<MetadataResponse>
    {
        public override string ToString() => $"{{brokers:[{brokers.ToStrings()}],topic_metadata:[{topic_metadata.ToStrings()}],cluster_id:{cluster_id},controller_id:{controller_id}}}";

        public static MetadataResponse FromBytes(IRequestContext context, ArraySegment<byte> bytes)
        {
            using (var reader = new KafkaReader(bytes)) {
                var brokers = new Server[reader.ReadInt32()];
                for (var b = 0; b < brokers.Length; b++) {
                    var brokerId = reader.ReadInt32();
                    var host = reader.ReadString();
                    var port = reader.ReadInt32();
                    string rack = null;
                    if (context.ApiVersion >= 1) {
                        rack = reader.ReadString();
                    }

                    brokers[b] = new Server(brokerId, host, port, rack);
                }

                string clusterId = null;
                if (context.ApiVersion >= 2) {
                    clusterId = reader.ReadString();
                }

                int? controllerId = null;
                if (context.ApiVersion >= 1) {
                    controllerId = reader.ReadInt32();
                }

                var topics = new Topic[reader.ReadInt32()];
                for (var t = 0; t < topics.Length; t++) {
                    var topicError = (ErrorCode) reader.ReadInt16();
                    var topicName = reader.ReadString();
                    bool? isInternal = null;
                    if (context.ApiVersion >= 1) {
                        isInternal = reader.ReadBoolean();
                    }

                    var partitions = new Partition[reader.ReadInt32()];
                    for (var p = 0; p < partitions.Length; p++) {
                        var partitionError = (ErrorCode) reader.ReadInt16();
                        var partitionId = reader.ReadInt32();
                        var leaderId = reader.ReadInt32();

                        var replicaCount = reader.ReadInt32();
                        var replicas = replicaCount.Repeat(reader.ReadInt32).ToArray();

                        var isrCount = reader.ReadInt32();
                        var isrs = isrCount.Repeat(reader.ReadInt32).ToArray();

                        partitions[p] = new Partition(partitionId, leaderId, partitionError, replicas, isrs);

                    }
                    topics[t] = new Topic(topicName, topicError, partitions, isInternal);
                }

                return new MetadataResponse(brokers, topics, controllerId, clusterId);
            }            
        }

        public MetadataResponse(IEnumerable<Server> brokers = null, IEnumerable<Topic> topics = null, int? controllerId = null, string clusterId = null)
        {
            this.brokers = ImmutableList<Server>.Empty.AddNotNullRange(brokers);
            topic_metadata = ImmutableList<Topic>.Empty.AddNotNullRange(topics);
            controller_id = controllerId;
            cluster_id = clusterId;
            Errors = ImmutableList<ErrorCode>.Empty.AddRange(topic_metadata.Select(t => t.topic_error_code));
        }

        public IImmutableList<ErrorCode> Errors { get; }

        public IImmutableList<Server> brokers { get; }
        public int? controller_id { get; }
        public string cluster_id { get; }
        public IImmutableList<Topic> topic_metadata { get; }

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
            return brokers.HasEqualElementsInOrder(other.brokers) 
                && controller_id == other.controller_id
                && cluster_id == other.cluster_id
                && topic_metadata.HasEqualElementsInOrder(other.topic_metadata);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = brokers?.Count.GetHashCode() ?? 0;
                hashCode = (hashCode*397) ^ (controller_id?.GetHashCode() ?? 0);
                hashCode = (hashCode*397) ^ (cluster_id?.GetHashCode() ?? 0);
                hashCode = (hashCode*397) ^ (topic_metadata?.Count.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        #endregion

        public class Topic : IEquatable<Topic>
        {
            public override string ToString() => $"{{topic:{topic},topic_error_code:{topic_error_code},partition_metadata:[{partition_metadata.ToStrings()}],is_internal:{is_internal}}}";

            public Topic(string topicName, ErrorCode errorCode = ErrorCode.NONE, IEnumerable<Partition> partitions = null, bool? isInternal = null)
            {
                topic_error_code = errorCode;
                topic = topicName;
                is_internal = isInternal;
                partition_metadata = ImmutableList<Partition>.Empty.AddNotNullRange(partitions);
            }

            public ErrorCode topic_error_code { get; }

            public string topic { get; }
            public bool? is_internal { get; }

            public IImmutableList<Partition> partition_metadata { get; }

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
                return topic_error_code == other.topic_error_code 
                    && string.Equals(topic, other.topic) 
                    && is_internal == other.is_internal
                    && partition_metadata.HasEqualElementsInOrder(other.partition_metadata);
            }

            /// <inheritdoc />
            public override int GetHashCode()
            {
                unchecked {
                    var hashCode = (int) topic_error_code;
                    hashCode = (hashCode*397) ^ (topic?.GetHashCode() ?? 0);
                    hashCode = (hashCode*397) ^ (is_internal?.GetHashCode() ?? 0);
                    hashCode = (hashCode*397) ^ (partition_metadata?.GetHashCode() ?? 0);
                    return hashCode;
                }
            }

            #endregion
        }

        public class Partition : IEquatable<Partition>
        {
            public override string ToString() => $"{{partition_id:{partition_id},partition_error_code:{partition_error_code},leader:{leader},replicas:[{replicas.ToStrings()}],isr:[{isr.ToStrings()}]}}";

            public Partition(int partitionId, int leaderId, ErrorCode errorCode = ErrorCode.NONE, IEnumerable<int> replicas = null, IEnumerable<int> isrs = null)
            {
                partition_error_code = errorCode;
                partition_id = partitionId;
                leader = leaderId;
                this.replicas = ImmutableList<int>.Empty.AddNotNullRange(replicas);
                isr = ImmutableList<int>.Empty.AddNotNullRange(isrs);
            }

            /// <summary>
            /// Error code.
            /// </summary>
            public ErrorCode partition_error_code { get; }

            /// <summary>
            /// The Id of the partition that this metadata describes.
            /// </summary>
            public int partition_id { get; }

            /// <summary>
            /// The node id for the kafka broker currently acting as leader for this partition. If no leader exists because we are in the middle of a leader election this id will be -1.
            /// </summary>
            public int leader { get; }

            public bool IsElectingLeader => leader == -1;

            /// <summary>
            /// The set of alive nodes that currently acts as slaves for the leader for this partition.
            /// </summary>
            public IImmutableList<int> replicas { get; }

            /// <summary>
            /// The set subset of the replicas that are "caught up" to the leader
            /// </summary>
            public IImmutableList<int> isr { get; }

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
                return partition_error_code == other.partition_error_code 
                    && partition_id == other.partition_id 
                    && leader == other.leader 
                    && replicas.HasEqualElementsInOrder(other.replicas) 
                    && isr.HasEqualElementsInOrder(other.isr);
            }

            /// <inheritdoc />
            public override int GetHashCode()
            {
                unchecked {
                    var hashCode = (int) partition_error_code;
                    hashCode = (hashCode*397) ^ partition_id;
                    hashCode = (hashCode*397) ^ leader;
                    hashCode = (hashCode*397) ^ (replicas?.Count.GetHashCode() ?? 0);
                    hashCode = (hashCode*397) ^ (isr?.Count.GetHashCode() ?? 0);
                    return hashCode;
                }
            }
        }

    }
}