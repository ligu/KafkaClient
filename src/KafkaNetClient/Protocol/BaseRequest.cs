namespace KafkaNet.Protocol
{
    public abstract class BaseRequest
    {
        /// <summary>
        /// Descriptive name of the source of the messages sent to kafka
        /// </summary>
        public string ClientId { get; set; } = "Kafka-Net";

        /// <summary>
        /// Value supplied will be passed back in the response by the server unmodified.
        /// It is useful for matching request and response between the client and server.
        /// </summary>
        public int CorrelationId { get; set; } = 1;

        /// <summary>
        /// This is a numeric version number for the api request. It allows the server to 
        /// properly interpret the request as the protocol evolves. Responses will always 
        /// be in the format corresponding to the request version.
        /// </summary>
        public short ApiVersion { get; set; } = 0;

        /// <summary>
        /// Flag which tells the broker call to expect a response for this request.
        /// </summary>
        public virtual bool ExpectResponse => true;
    }
}