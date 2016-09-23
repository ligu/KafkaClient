namespace KafkaClient.Protocol
{
    /// <summary>
    /// A Protocol for requesting which versions are supported for each api key
    /// </summary>
    public class ApiVersionsRequest : Request, IRequest<ApiVersionsResponse>
    {
        public ApiVersionsRequest() 
            : base(ApiKeyRequestType.ApiVersions)
        {
        }
    }
}