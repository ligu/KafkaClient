using KafkaNet.Common;
using System.Collections.Generic;

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
            return EncodeOffsetRequest(this);
        }

        public IEnumerable<ApiVersionsResponse> Decode(byte[] payload)
        {
            return new [] { DecodeOffsetResponse(payload) };
        }

        private KafkaDataPayload EncodeOffsetRequest(ApiVersionsRequest request)
        {
            using (var message = EncodeHeader(request)) {
                return new KafkaDataPayload {
                    Buffer = message.Payload(),
                    CorrelationId = request.CorrelationId,
                    ApiKey = ApiKey
                };
            }
        }

        private ApiVersionsResponse DecodeOffsetResponse(byte[] data)
        {
            using (var stream = new BigEndianBinaryReader(data)) {
                var correlationId = stream.ReadInt32();
                var errorCode = (ErrorResponseCode)stream.ReadInt16();

                var apiKeys = new ApiVersionSupport[stream.ReadInt32()];
                for (int i = 0; i < apiKeys.Length; i++) {
                    var apiKey = (ApiKeyRequestType)stream.ReadInt16();
                    var minVersion = stream.ReadInt16();
                    var maxVersion = stream.ReadInt16();
                    apiKeys[i] = new ApiVersionSupport(apiKey, minVersion, maxVersion);
                }
                return new ApiVersionsResponse(correlationId, errorCode, apiKeys);
            }
        }
    }

    public class ApiVersionSupport
    {
        public ApiVersionSupport(ApiKeyRequestType apiKey, short minVersion, short maxVersion)
        {
            ApiKey = apiKey;
            MinVersion = minVersion;
            MaxVersion = maxVersion;
        }

        /// <summary>
        /// API key.
        /// </summary>
        public ApiKeyRequestType ApiKey { get; } 

        /// <summary>
        /// Minimum supported version.
        /// </summary>
        public short MinVersion { get; }

        /// <summary>
        /// Maximum supported version.
        /// </summary>
        public short MaxVersion { get; }
    }

    public class ApiVersionsResponse
    {
        public ApiVersionsResponse(int correlationId, ErrorResponseCode errorCode, IEnumerable<ApiVersionSupport> supportedVersions)
        {
            CorrelationId = correlationId;
            ErrorCode = errorCode;
            SupportedVersions = supportedVersions;
        }

        public int CorrelationId { get; }

        public ErrorResponseCode ErrorCode { get; }

        public IEnumerable<ApiVersionSupport> SupportedVersions { get; }
    }
}