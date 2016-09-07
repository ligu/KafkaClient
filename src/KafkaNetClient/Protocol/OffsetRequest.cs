using KafkaNet.Common;
using System.Collections.Generic;

namespace KafkaNet.Protocol
{
    /// <summary>
    /// A funky Protocol for requesting the starting offset of each segment for the requested partition
    /// </summary>
    public class OffsetRequest : BaseRequest, IKafkaRequest<OffsetResponse>
    {

        public ApiKeyRequestType ApiKey => ApiKeyRequestType.Offset;
        public List<Offset> Offsets { get; set; }

        public KafkaDataPayload Encode()
        {
            if (Offsets == null) {
                Offsets = new List<Offset>();
            }

            return new KafkaDataPayload {
                Buffer = Protocol.EncodeRequest.OffsetRequest(this),
                CorrelationId = CorrelationId,
                ApiKey = ApiKey
            };
        }

        public OffsetResponse Decode(byte[] payload)
        {
            return Protocol.DecodeResponse.OffsetResponse(ApiVersion, payload);
        }
    }
}