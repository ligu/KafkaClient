using KafkaClient.Connections;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// An exception caused by a Kafka Request
    /// </summary>
    public class RequestException : KafkaException
    {
        public RequestException(ApiKey apiKey, ErrorCode errorCode, Endpoint endpoint, string message = null)
            : base($"{endpoint} returned {errorCode} for {apiKey} request: {message}")
        {
            _apiKey = apiKey;
            ErrorCode = errorCode;
            _endpoint = endpoint;
        }

        // ReSharper disable NotAccessedField.Local -- for debugging
        private readonly ApiKey _apiKey;
        private readonly Endpoint _endpoint;
        // ReSharper restore NotAccessedField.Local

        public ErrorCode ErrorCode { get; }
    }
}