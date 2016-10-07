using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// A funky Protocol for requesting the starting offset of each segment for the requested partition
    /// </summary>
    public class OffsetRequest : Request, IRequest<OffsetResponse>, IEquatable<OffsetRequest>
    {
        public OffsetRequest(params Offset[] offsets)
            : this((IEnumerable<Offset>)offsets)
        {
        }

        public OffsetRequest(IEnumerable<Offset> offsets) 
            : base(ApiKeyRequestType.Offset)
        {
            Offsets = ImmutableList<Offset>.Empty.AddNotNullRange(offsets);
        }

        public IImmutableList<Offset> Offsets { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as OffsetRequest);
        }

        /// <inheritdoc />
        public bool Equals(OffsetRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Offsets.HasEqualElementsInOrder(other.Offsets);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return Offsets?.GetHashCode() ?? 0;
        }

        /// <inheritdoc />
        public static bool operator ==(OffsetRequest left, OffsetRequest right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(OffsetRequest left, OffsetRequest right)
        {
            return !Equals(left, right);
        }
    }
}