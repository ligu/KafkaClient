using System;
using System.Collections.Generic;
using System.Collections.Immutable;

namespace KafkaNet.Protocol
{
    public class OffsetTopic : TopicResponse, IEquatable<OffsetTopic>
    {
        public OffsetTopic(string topic, int partitionId, ErrorResponseCode errorCode = ErrorResponseCode.NoError, IEnumerable<long> offsets = null) 
            : base(topic, partitionId, errorCode)
        {
            Offsets = offsets != null ? ImmutableList<long>.Empty.AddRange(offsets) : ImmutableList<long>.Empty;
        }

        public ImmutableList<long> Offsets { get; }

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
                   && Equals(Offsets, other.Offsets);
        }

        public override int GetHashCode()
        {
            unchecked {
                return (base.GetHashCode()*397) ^ (Offsets?.GetHashCode() ?? 0);
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