using System;

namespace KafkaClient.Protocol
{
    public class Offset : Topic, IEquatable<Offset>
    {
        public Offset(string topicName, int partitionId, long timestamp = LatestTime, int maxOffsets = DefaultMaxOffsets) : base(topicName, partitionId)
        {
            Timestamp = timestamp;
            MaxOffsets = maxOffsets;
        }

        /// <summary>
        /// Used to ask for all messages before a certain time (ms). There are two special values.
        /// Specify -1 to receive the latest offsets and -2 to receive the earliest available offset.
        /// Note that because offsets are pulled in descending order, asking for the earliest offset will always return you a single element.
        /// </summary>
        public long Timestamp { get; }

        /// <summary>
        /// Only applies to version 1 (Kafka 0.10.1 and below)
        /// </summary>
        public int MaxOffsets { get; }

        public const long LatestTime = -1L;
        public const long EarliestTime = -2L;
        public const int DefaultMaxOffsets = 1;

        #region Equality

        public override bool Equals(object obj)
        {
            return Equals(obj as Offset);
        }

        public bool Equals(Offset other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                && Timestamp == other.Timestamp 
                && MaxOffsets == other.MaxOffsets;
        }

        public override int GetHashCode()
        {
            unchecked {
                int hashCode = base.GetHashCode();
                hashCode = (hashCode*397) ^ Timestamp.GetHashCode();
                hashCode = (hashCode*397) ^ MaxOffsets;
                return hashCode;
            }
        }

        public static bool operator ==(Offset left, Offset right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(Offset left, Offset right)
        {
            return !Equals(left, right);
        }

        #endregion
        
    }
}