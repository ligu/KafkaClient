using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    public class OffsetTopic : TopicResponse, IEquatable<OffsetTopic>
    {
        public OffsetTopic(string topic, int partitionId, ErrorResponseCode errorCode = ErrorResponseCode.None, long offset = -1, DateTime? timestamp = null) 
            : base(topic, partitionId, errorCode)
        {
            Offset = offset;
            Timestamp = timestamp;
        }

        /// <summary>
        /// The timestamp associated with the returned offset.
        /// This only applies to version 1 and above.
        /// </summary>
        public DateTime? Timestamp { get; set; }

        /// <summary>
        /// The offset found.
        /// </summary>
        public long Offset { get; set; }

        #region Equality

        public override bool Equals(object obj)
        {
            return Equals(obj as OffsetTopic);
        }

        public bool Equals(OffsetTopic other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                && Timestamp == other.Timestamp
                && Offset == other.Offset;
        }

        public override int GetHashCode()
        {
            unchecked {
                var hashCode = base.GetHashCode();
                hashCode = (hashCode*397) ^ (Timestamp?.GetHashCode() ?? 0);
                hashCode = (hashCode*397) ^ Offset.GetHashCode();
                return hashCode;
            }
        }

        public static bool operator ==(OffsetTopic left, OffsetTopic right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(OffsetTopic left, OffsetTopic right)
        {
            return !Equals(left, right);
        }
        
        #endregion

    }
}