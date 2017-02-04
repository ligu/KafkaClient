using System;
using KafkaClient.Connections;

namespace KafkaClient
{
    public class TopicConnection : ServerConnection, IEquatable<TopicConnection>
    {
        public TopicConnection(string topicName, int partitionId, int serverId, IConnection connection) 
            : base(serverId, connection)
        {
            TopicName = topicName;
            PartitionId = partitionId;
        }

        public string TopicName { get; }
        public int PartitionId { get; }

        public override string ToString() => $"{base.ToString()} Topic:{TopicName},PartitionId:{PartitionId}";

        #region Equality

        public override bool Equals(object obj)
        {
            return base.Equals(obj);
        }

        public bool Equals(TopicConnection other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) && string.Equals(TopicName, other.TopicName) && PartitionId == other.PartitionId;
        }

        public override int GetHashCode()
        {
            unchecked {
                var hashCode = base.GetHashCode();
                hashCode = (hashCode * 397) ^ (TopicName?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ PartitionId;
                return hashCode;
            }
        }

        #endregion
    }
}