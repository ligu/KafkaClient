using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    public class FetchTopicResponse : TopicResponse, IEquatable<FetchTopicResponse>
    {
        public FetchTopicResponse(string topic, int partitionId, long highWaterMark, ErrorResponseCode errorCode = ErrorResponseCode.None, IEnumerable<Message> messages = null)
            : base(topic, partitionId, errorCode)
        {
            HighWaterMark = highWaterMark;
            Messages = ImmutableList<Message>.Empty.AddNotNullRange(messages);
        }

        /// <summary>
        /// The offset at the end of the log for this partition. This can be used by the client to determine how many messages behind the end of the log they are.
        /// </summary>
        public long HighWaterMark { get; }

        public IImmutableList<Message> Messages { get; }

        #region Equality

        public override bool Equals(object obj)
        {
            return Equals(obj as FetchTopicResponse);
        }

        public bool Equals(FetchTopicResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                && HighWaterMark == other.HighWaterMark 
                && Equals(Messages, other.Messages);
        }

        public override int GetHashCode()
        {
            unchecked {
                int hashCode = base.GetHashCode();
                hashCode = (hashCode*397) ^ HighWaterMark.GetHashCode();
                hashCode = (hashCode*397) ^ (Messages?.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        public static bool operator ==(FetchTopicResponse left, FetchTopicResponse right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(FetchTopicResponse left, FetchTopicResponse right)
        {
            return !Equals(left, right);
        }

        #endregion

    }
}