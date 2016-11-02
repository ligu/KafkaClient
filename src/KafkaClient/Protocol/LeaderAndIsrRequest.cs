using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// LeaderAndIsrRequest => ControllerId ControllerEpoch [partition_state] [live_leaders] 
    ///  ControllerId => INT32
    ///  ControllerEpoch => INT32
    ///  partition_state => topic partition controller_epoch leader leader_epoch [isr] zk_version [replicas] 
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
        public LeaderAndIsrRequest(int controllerId, int controllerEpoch, IEnumerable<PartitionState> partitionStates, IEnumerable<Broker> liveLeaders) : base(ApiKeyRequestType.LeaderAndIsr)
        {
            ControllerId = controllerId;
            ControllerEpoch = controllerEpoch;
            PartitionStates = ImmutableList<PartitionState>.Empty.AddNotNullRange(partitionStates);
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

        public IImmutableList<PartitionState> PartitionStates { get; }
        public IImmutableList<Broker> LiveLeaders { get; }

        #region Equality

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

        #endregion

        public class PartitionState : TopicPartition, IEquatable<PartitionState>
        {
            public PartitionState(string topicName, int partitionId, int controllerEpoch, int leader, int leaderEpoch, int zookeeperVersion, IEnumerable<int> isrs = null, IEnumerable<int> replicas = null) 
                : base(topicName, partitionId)
            {
                ControllerEpoch = controllerEpoch;
                Leader = leader;
                LeaderEpoch = leaderEpoch;
                Isrs = ImmutableList<int>.Empty.AddNotNullRange(isrs);
                ZookeeperVersion = zookeeperVersion;
                Replicas = ImmutableList<int>.Empty.AddNotNullRange(replicas);
            }

            public int ControllerEpoch { get; }

            /// <summary>
            /// The broker id for the leader.
            /// </summary>
            public int Leader { get; }

            public int LeaderEpoch { get; }

            /// <summary>
            /// The in sync replica ids.
            /// </summary>
            public IImmutableList<int> Isrs { get; }

            /// <summary>
            /// The ZK version.
            /// </summary>
            public int ZookeeperVersion { get; }

            public IImmutableList<int> Replicas { get; }

            public override bool Equals(object obj)
            {
                return Equals(obj as PartitionState);
            }

            public bool Equals(PartitionState other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return base.Equals(other) 
                    && ControllerEpoch == other.ControllerEpoch 
                    && Leader == other.Leader 
                    && LeaderEpoch == other.LeaderEpoch 
                    && ZookeeperVersion == other.ZookeeperVersion 
                    && Isrs.HasEqualElementsInOrder(other.Isrs) 
                    && Replicas.HasEqualElementsInOrder(other.Replicas);
            }

            public override int GetHashCode()
            {
                unchecked {
                    int hashCode = base.GetHashCode();
                    hashCode = (hashCode * 397) ^ ControllerEpoch;
                    hashCode = (hashCode * 397) ^ Leader;
                    hashCode = (hashCode * 397) ^ LeaderEpoch;
                    hashCode = (hashCode * 397) ^ (Isrs?.GetHashCode() ?? 0);
                    hashCode = (hashCode * 397) ^ ZookeeperVersion;
                    hashCode = (hashCode * 397) ^ (Replicas?.GetHashCode() ?? 0);
                    return hashCode;
                }
            }

            public static bool operator ==(PartitionState left, PartitionState right)
            {
                return Equals(left, right);
            }

            public static bool operator !=(PartitionState left, PartitionState right)
            {
                return !Equals(left, right);
            }

        }

    }
}