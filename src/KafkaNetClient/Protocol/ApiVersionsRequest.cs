using System;
using System.Diagnostics.Contracts;

namespace KafkaNet.Protocol
{
    /// <summary>
    /// A Protocol for requesting which versions are supported for each api key
    /// </summary>
    public class ApiVersionsRequest : BaseRequest, IKafkaRequest<ApiVersionsResponse>
    {
        public ApiKeyRequestType ApiKey => ApiKeyRequestType.ApiVersions;

        public KafkaDataPayload Encode()
        {
            return new KafkaDataPayload {
                Buffer = EncodeRequest.ApiVersionsRequest(this),
                CorrelationId = CorrelationId,
                ApiKey = ApiKey
            };
        }

        public ApiVersionsResponse Decode(byte[] payload)
        {
            return DecodeResponse.ApiVersionsResponse(ApiVersion, payload);
        }
    }
}