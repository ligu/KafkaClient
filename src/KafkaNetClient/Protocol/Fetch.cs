using System;

namespace KafkaNet.Protocol
{
    public class Fetch : Topic, IEquatable<Fetch>
    {
        public Fetch(string topicName, int partitionId, long offset, int? maxBytes = null)
            : base(topicName, partitionId)
        {
            Offset = offset;
            MaxBytes = maxBytes.GetValueOrDefault(FetchRequest.DefaultMinBlockingByteBufferSize * 8);
        }

        /// <summary>
        /// The offset to begin this fetch from.
        /// </summary>
        public long Offset { get; }

        /// <summary>
        /// The maximum bytes to include in the message set for this partition. This helps bound the size of the response.
        /// </summary>
        public int MaxBytes { get; }

        #region Equality

        public override bool Equals(object obj)
        {
            return Equals(obj as Fetch);
        }

        public bool Equals(Fetch other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                && Offset == other.Offset 
                && MaxBytes == other.MaxBytes;
        }

        public override int GetHashCode()
        {
            unchecked {
                int hashCode = base.GetHashCode();
                hashCode = (hashCode*397) ^ Offset.GetHashCode();
                hashCode = (hashCode*397) ^ MaxBytes;
                return hashCode;
            }
        }

        public static bool operator ==(Fetch left, Fetch right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(Fetch left, Fetch right)
        {
            return !Equals(left, right);
        }

        #endregion
    }
}