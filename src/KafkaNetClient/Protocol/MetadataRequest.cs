using KafkaNet.Common;
using System.Collections.Generic;

namespace KafkaNet.Protocol
{
    public class MetadataRequest : BaseRequest, IKafkaRequest<MetadataResponse>
    {
        /// <summary>
        /// Indicates the type of kafka encoding this request is
        /// </summary>
        public ApiKeyRequestType ApiKey => ApiKeyRequestType.MetaData;

        /// <summary>
        /// The list of topics to get metadata for.
        /// </summary>
        public List<string> Topics { get; set; }

        public KafkaDataPayload Encode()
        {
            if (Topics == null) {
                Topics = new List<string>();
            }

            return new KafkaDataPayload {
                Buffer = EncodeRequest.MetadataRequest(this),
                CorrelationId = CorrelationId,
                ApiKey = ApiKey
            };
        }

        public MetadataResponse Decode(byte[] payload)
        {
            return DecodeResponse.MetadataResponse(ApiVersion, payload);
        }
    }
}