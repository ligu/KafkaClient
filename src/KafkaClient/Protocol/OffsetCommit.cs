using System;

namespace KafkaNet.Protocol
{
    public class OffsetCommit : Topic, IEquatable<OffsetCommit>
    {
        public OffsetCommit(string topicName, int partitionId, long offset, string metadata = null, long? timeStamp = null) 
            : base(topicName, partitionId)
        {
            Offset = offset;
            TimeStamp = timeStamp;
            Metadata = metadata;
        }

        /// <summary>
        /// The offset number to commit as completed.
        /// </summary>
        public long Offset { get; }

        /// <summary>
        /// If the time stamp field is set to -1, then the broker sets the time stamp to the receive time before committing the offset.
        /// Only version 1 (0.8.2)
        /// </summary>
        public long? TimeStamp { get; }

        /// <summary>
        /// Descriptive metadata about this commit.
        /// </summary>
        public string Metadata { get; }

        public override bool Equals(object obj)
        {
            return Equals(obj as OffsetCommit);
        }

        public bool Equals(OffsetCommit other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                && Offset == other.Offset 
                && string.Equals(Metadata, other.Metadata);
        }

        public override int GetHashCode()
        {
            unchecked {
                int hashCode = base.GetHashCode();
                hashCode = (hashCode*397) ^ Offset.GetHashCode();
                hashCode = (hashCode*397) ^ (Metadata?.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        public static bool operator ==(OffsetCommit left, OffsetCommit right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(OffsetCommit left, OffsetCommit right)
        {
            return !Equals(left, right);
        }
    }
}