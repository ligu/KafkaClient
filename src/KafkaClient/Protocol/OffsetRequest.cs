using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// A funky Protocol for requesting the starting offset of each segment for the requested partition
    /// </summary>
    public class OffsetRequest : Request, IRequest<OffsetResponse>
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
    }
}