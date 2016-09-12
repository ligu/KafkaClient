namespace KafkaNet.Protocol
{
    public interface IKafkaRequest<T> : IKafkaRequest
        where T : IKafkaResponse
    {
        
    }

    public interface IKafkaRequest
    {
        /// <summary>
        /// Indicates this request should wait for a response from the broker
        /// </summary>
        bool ExpectResponse { get; }

        /// <summary>
        /// Enum identifying the specific type of request message being represented.
        /// </summary>
        ApiKeyRequestType ApiKey { get; }
    }
}