using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    public class LeaderAndIsrStates : Topic, IEquatable<LeaderAndIsrStates>
    {
        public LeaderAndIsrStates(string topicName, int partitionId, int controllerEpoch, int leader, int leaderEpoch, int zookeeperVersion, IEnumerable<int> isrs = null, IEnumerable<int> replicas = null) 
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
            return Equals(obj as LeaderAndIsrStates);
        }

        public bool Equals(LeaderAndIsrStates other)
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

        public static bool operator ==(LeaderAndIsrStates left, LeaderAndIsrStates right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(LeaderAndIsrStates left, LeaderAndIsrStates right)
        {
            return !Equals(left, right);
        }

    }
}