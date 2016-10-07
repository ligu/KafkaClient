using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// StopReplica Request => controller_id controller_epoch delete_partitions [topic partition] 
    ///   controller_id => INT32    -- The controller id.
    ///   controller_epoch => INT32 -- The controller epoch.
    ///   delete_partitions => INT8 -- Boolean which indicates if replica's partitions must be deleted.
    ///   topic => STRING           -- Topic name.
    ///   partition => INT32        -- Topic partition id.
    /// </summary>
    public class StopReplicaRequest : Request, IRequest<StopReplicaResponse>, IEquatable<StopReplicaRequest>
    {
        /// <inheritdoc />
        public StopReplicaRequest(int controllerId, int controllerEpoch, IEnumerable<Topic> topics, bool deletePartitions = true) : base(ApiKeyRequestType.StopReplica)
        {
            ControllerId = controllerId;
            ControllerEpoch = controllerEpoch;
            DeletePartitions = deletePartitions;
            Topics = ImmutableList<Topic>.Empty.AddNotNullRange(topics);
        }

        /// <summary>
        /// The controller id.
        /// </summary>
        public int ControllerId { get; }

        /// <summary>
        /// The controller epoch.
        /// </summary>
        public int ControllerEpoch { get; }

        /// <summary>
        /// Boolean which indicates if replica's partitions must be deleted.
        /// </summary>
        public bool DeletePartitions { get; }

        /// <summary>
        /// The topic/partitions to be stopped.
        /// </summary>
        public IImmutableList<Topic> Topics { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as StopReplicaRequest);
        }

        /// <inheritdoc />
        public bool Equals(StopReplicaRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                   && ControllerId == other.ControllerId 
                   && ControllerEpoch == other.ControllerEpoch 
                   && DeletePartitions == other.DeletePartitions 
                   && Topics.HasEqualElementsInOrder(other.Topics);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                int hashCode = base.GetHashCode();
                hashCode = (hashCode*397) ^ ControllerId;
                hashCode = (hashCode*397) ^ ControllerEpoch;
                hashCode = (hashCode*397) ^ DeletePartitions.GetHashCode();
                hashCode = (hashCode*397) ^ (Topics?.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        /// <inheritdoc />
        public static bool operator ==(StopReplicaRequest left, StopReplicaRequest right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(StopReplicaRequest left, StopReplicaRequest right)
        {
            return !Equals(left, right);
        }
    }
}