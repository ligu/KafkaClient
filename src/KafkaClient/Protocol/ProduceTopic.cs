using System;

namespace KafkaNet.Protocol
{
    public class ProduceTopic : TopicResponse, IEquatable<ProduceTopic>
    {
        public ProduceTopic(string topic, int partitionId, ErrorResponseCode errorCode, long offset, DateTime? timestamp = null)
            : base(topic, partitionId, errorCode)
        {
            Offset = offset;
            Timestamp = timestamp;
        }

        /// <summary>
        /// The offset number to commit as completed.
        /// </summary>
        public long Offset { get; }

        /// <summary>
        /// If LogAppendTime is used for the topic, this is the timestamp assigned by the broker to the message set. 
        /// All the messages in the message set have the same timestamp.
        /// If CreateTime is used, this field is always -1. The producer can assume the timestamp of the messages in the 
        /// produce request has been accepted by the broker if there is no error code returned.
        /// </summary>
        public DateTime? Timestamp { get; }

        #region Equality

        public override bool Equals(object obj)
        {
            return Equals(obj as ProduceTopic);
        }

        public bool Equals(ProduceTopic other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                && Offset == other.Offset 
                && Timestamp.Equals(other.Timestamp);
        }

        public override int GetHashCode()
        {
            unchecked {
                int hashCode = base.GetHashCode();
                hashCode = (hashCode*397) ^ Offset.GetHashCode();
                hashCode = (hashCode*397) ^ Timestamp.GetHashCode();
                return hashCode;
            }
        }

        public static bool operator ==(ProduceTopic left, ProduceTopic right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(ProduceTopic left, ProduceTopic right)
        {
            return !Equals(left, right);
        }

        #endregion

    }
}