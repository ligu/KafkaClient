namespace KafkaNet.Protocol
{
    /// <summary>
    /// A Protocol for requesting which versions are supported for each api key
    /// </summary>
    public class ApiVersionsRequest : KafkaRequest
    {
        public ApiVersionsRequest() 
            : base(ApiKeyRequestType.ApiVersions)
        {
        }
    }
}