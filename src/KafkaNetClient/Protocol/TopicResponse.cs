using System;

namespace KafkaNet.Protocol
{
    public class TopicResponse : Topic, IEquatable<TopicResponse>
    {
        public TopicResponse(string topicName, int partitionId, ErrorResponseCode error)
            : base(topicName, partitionId)
        {
            Error = error;
        }

        /// <summary>
        /// Error response code.
        /// </summary>
        public ErrorResponseCode Error { get; }

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
                && Error == other.Error;
        }

        public override int GetHashCode()
        {
            unchecked {
                var hashCode = base.GetHashCode();
                hashCode = (hashCode*397) ^ (int) Error;
                return hashCode;
            }
        }

        public static bool operator ==(TopicResponse left, TopicResponse right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(TopicResponse left, TopicResponse right)
        {
            return !Equals(left, right);
        }

        #endregion

        public override string ToString() => $"{base.ToString()} ErrorCode: {Error}";
    }
}