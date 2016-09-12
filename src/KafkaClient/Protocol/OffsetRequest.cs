using System.Collections.Generic;
using System.Collections.Immutable;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// A funky Protocol for requesting the starting offset of each segment for the requested partition
    /// </summary>
    public class OffsetRequest : KafkaRequest, IKafkaRequest<OffsetResponse>
    {
        public OffsetRequest(params Offset[] offsets)
            : this((IEnumerable<Offset>)offsets)
        {
        }

        public OffsetRequest(IEnumerable<Offset> offsets) 
            : base(ApiKeyRequestType.Offset)
        {
            Offsets = offsets != null ? ImmutableList<Offset>.Empty.AddRange(offsets) : ImmutableList<Offset>.Empty;
        }

        public ImmutableList<Offset> Offsets { get; }
    }
}