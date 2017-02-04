using System;

namespace KafkaClient.Protocol
{
    public class TopicResponse : TopicPartition, IEquatable<TopicResponse>
    {
        public TopicResponse(string topicName, int partitionId, ErrorCode errorCode)
            : base(topicName, partitionId)
        {
            ErrorCode = errorCode;
        }

        /// <summary>
        /// Error response code.
        /// </summary>
        public ErrorCode ErrorCode { get; }

        #region Equality

        public override bool Equals(object obj)
        {
            return Equals(obj as TopicResponse);
        }

        public bool Equals(TopicResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other)
                && ErrorCode == other.ErrorCode;
        }

        public override int GetHashCode()
        {
            unchecked {
                var hashCode = base.GetHashCode();
                hashCode = (hashCode*397) ^ (int) ErrorCode;
                return hashCode;
            }
        }

        #endregion

        public override string ToString() => $"{{TopicName:{TopicName},PartitionId:{PartitionId},ErrorCode:{ErrorCode}}}";
    }
}