using System.Collections.Generic;
using System.Collections.Immutable;

namespace KafkaNet.Protocol
{
    /// <summary>
    /// A funky Protocol for requesting the starting offset of each segment for the requested partition
    /// </summary>
    public class OffsetRequest : KafkaRequest
    {
        public OffsetRequest(IEnumerable<Offset> offsets) 
            : base(ApiKeyRequestType.Offset)
        {
            Offsets = offsets != null ? ImmutableList<Offset>.Empty.AddRange(offsets) : ImmutableList<Offset>.Empty;
        }

        public ImmutableList<Offset> Offsets { get; }
    }
}