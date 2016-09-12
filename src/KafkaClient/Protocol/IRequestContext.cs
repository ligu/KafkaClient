namespace KafkaNet.Protocol
{
    public interface IRequestContext
    {
        /// <summary>
        /// Descriptive name used to identify the source of this request.
        /// </summary>
        string ClientId { get; }

        /// <summary>
        /// Id which will be echoed back by Kafka to correlate responses to this request.  Usually automatically assigned by driver.
        /// </summary>
        int CorrelationId { get; }

        /// <summary>
        /// This is a numeric version number for the api request. It allows the server to properly interpret the request as the protocol evolves. Responses will always be in the format corresponding to the request version.
        /// </summary>
        short ApiVersion { get; }
    }
}