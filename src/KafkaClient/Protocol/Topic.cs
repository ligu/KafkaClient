using System;

namespace KafkaClient.Protocol
{
    public class Topic : IEquatable<Topic>
    {
        public Topic(string topicName, int partitionId)
        {
            TopicName = topicName;
            PartitionId = partitionId;
        }

        /// <summary>
        /// The topic name.
        /// </summary>
        public string TopicName { get; }

        /// <summary>
        /// The partition id.
        /// </summary>
        public int PartitionId { get; }

        #region Equality

        public override bool Equals(object obj)
        {
            return Equals(obj as Topic);
        }

        public bool Equals(Topic other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return string.Equals(TopicName, other.TopicName) 
                   && PartitionId == other.PartitionId;
        }

        public override int GetHashCode()
        {
            unchecked {
                var hashCode = TopicName?.GetHashCode() ?? 0;
                hashCode = (hashCode*397) ^ PartitionId;
                return hashCode;
            }
        }

        public static bool operator ==(Topic left, Topic right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(Topic left, Topic right)
        {
            return !Equals(left, right);
        }

        #endregion

        public override string ToString() => $"Topic: {TopicName} Partition: {PartitionId}";
    }
}