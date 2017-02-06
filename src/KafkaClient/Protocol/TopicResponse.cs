using System;
// ReSharper disable InconsistentNaming

namespace KafkaClient.Protocol
{
    public class TopicResponse : TopicPartition, IEquatable<TopicResponse>
    {
        public TopicResponse(string topicName, int partitionId, ErrorCode errorCode)
            : base(topicName, partitionId)
        {
            error_code = errorCode;
        }

        /// <summary>
        /// Error response code.
        /// </summary>
        public ErrorCode error_code { get; }

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
                && error_code == other.error_code;
        }

        public override int GetHashCode()
        {
            unchecked {
                var hashCode = base.GetHashCode();
                hashCode = (hashCode*397) ^ (int) error_code;
                return hashCode;
            }
        }

        #endregion

        public override string ToString() => $"{{topic:{topic},partition_id:{partition_id},error_code:{error_code}}}";
    }
}