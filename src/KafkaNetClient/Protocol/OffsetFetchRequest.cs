using System.Collections.Generic;

namespace KafkaNet.Protocol
{
    /// <summary>
    /// Class that represents both the request and the response from a kafka server of requesting a stored offset value
    /// for a given consumer group.  Essentially this part of the api allows a user to save/load a given offset position
    /// under any abritrary name.
    /// </summary>
    public class OffsetFetchRequest : BaseRequest, IKafkaRequest<OffsetFetchResponse>
    {
        public ApiKeyRequestType ApiKey => ApiKeyRequestType.OffsetFetch;

        public string ConsumerGroup { get; set; }

        public List<Topic> Topics { get; set; }

        public KafkaDataPayload Encode()
        {
            if (Topics == null) {
                Topics = new List<Topic>();
            }

            return new KafkaDataPayload {
                Buffer = EncodeRequest.OffsetFetchRequest(this),
                CorrelationId = CorrelationId,
                ApiKey = ApiKey
            };
        }

        public OffsetFetchResponse Decode(byte[] payload)
        {
            return DecodeResponse.OffsetFetchResponse(ApiVersion, payload);
        }
    }
}