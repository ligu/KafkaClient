namespace KafkaClient.Protocol
{
    public abstract class Request : IRequest
    {
        protected Request(ApiKeyRequestType apiKey, bool expectResponse = true)
        {
            ApiKey = apiKey;
            ExpectResponse = expectResponse;
        }

        /// <summary>
        /// Enum identifying the specific type of request message being represented.
        /// </summary>
        public ApiKeyRequestType ApiKey { get; }

        /// <summary>
        /// Flag which tells the broker call to expect a response for this request.
        /// </summary>
        public bool ExpectResponse { get; }
    }
}