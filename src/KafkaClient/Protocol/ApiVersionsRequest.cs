using System;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// ApiVersions Request => 
    ///
    /// From http://kafka.apache.org/protocol.html#protocol_messages
    /// A Protocol for requesting which versions are supported for each api key
    /// </summary>
    public class ApiVersionsRequest : Request, IRequest<ApiVersionsResponse>
    {
        public override string ToString() => $"{{Api:{ApiKey}}}";

        public ApiVersionsResponse ToResponse(IRequestContext context, ArraySegment<byte> bytes) => ApiVersionsResponse.FromBytes(context, bytes);

        public ApiVersionsRequest() 
            : base(ApiKey.ApiVersions)
        {
        }
    }
}