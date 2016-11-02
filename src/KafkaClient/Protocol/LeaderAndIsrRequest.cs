using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// LeaderAndIsrRequest => ControllerId ControllerEpoch [partition_states] [live_leaders] 
    ///  ControllerId => INT32
    ///  ControllerEpoch => INT32
    ///  partition_states => topic partition controller_epoch leader leader_epoch [isr] zk_version [replicas] 
    ///    topic => STRING
    ///    partition => INT32
    ///    controller_epoch => INT32
    ///    leader => INT32
    ///    leader_epoch => INT32
    ///    isr => INT32
    ///    zk_version => INT32
    ///    replicas => INT32
    ///  live_leaders => id host port 
    ///    id => INT32
    ///    host => STRING
    ///    port => INT32
    /// 
    /// </summary>
    public class LeaderAndIsrRequest : Request, IRequest<LeaderAndIsrResponse>, IEquatable<LeaderAndIsrRequest>
    {
        public LeaderAndIsrRequest(int controllerId, int controllerEpoch, IEnumerable<LeaderAndIsrStates> partitionStates, IEnumerable<Broker> liveLeaders) : base(ApiKeyRequestType.LeaderAndIsr)
        {
            ControllerId = controllerId;
            ControllerEpoch = controllerEpoch;
            PartitionStates = ImmutableList<LeaderAndIsrStates>.Empty.AddNotNullRange(partitionStates);
            LiveLeaders = ImmutableList<Broker>.Empty.AddNotNullRange(liveLeaders);
        }

        /// <summary>
        /// The controller id.
        /// </summary>
        public int ControllerId { get; }

        /// <summary>
        /// The controller epoch.
        /// </summary>
        public int ControllerEpoch { get; }

        public IImmutableList<LeaderAndIsrStates> PartitionStates { get; }
        public IImmutableList<Broker> LiveLeaders { get; }

        public override bool Equals(object obj)
        {
            return Equals(obj as LeaderAndIsrRequest);
        }

        public bool Equals(LeaderAndIsrRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                && ControllerId == other.ControllerId 
                && ControllerEpoch == other.ControllerEpoch 
                && PartitionStates.HasEqualElementsInOrder(other.PartitionStates)
                && LiveLeaders.HasEqualElementsInOrder(other.LiveLeaders);
        }

        public override int GetHashCode()
        {
            unchecked {
                int hashCode = base.GetHashCode();
                hashCode = (hashCode * 397) ^ ControllerId;
                hashCode = (hashCode * 397) ^ ControllerEpoch;
                hashCode = (hashCode * 397) ^ (PartitionStates?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (LiveLeaders?.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        public static bool operator ==(LeaderAndIsrRequest left, LeaderAndIsrRequest right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(LeaderAndIsrRequest left, LeaderAndIsrRequest right)
        {
            return !Equals(left, right);
        }
    }
}