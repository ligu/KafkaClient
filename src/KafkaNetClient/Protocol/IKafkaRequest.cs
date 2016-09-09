namespace KafkaNet.Protocol
{
    /// <summary>
    /// KafkaRequest represents a Kafka request messages as an object which can Encode itself into the appropriate
    /// binary request and Decode any responses to that request.
    /// </summary>
    /// <typeparam name="T">The type of the KafkaResponse expected back from the request.</typeparam>
    public interface IKafkaRequest<out T> : IKafkaRequest 
        where T : IKafkaResponse
    {
        /// <summary>
        /// Encode this request into the Kafka wire protocol.
        /// </summary>
        /// <returns>Byte[] representing the binary wire protocol of this request.</returns>
        KafkaDataPayload Encode();

        /// <summary>
        /// Decode a response payload from Kafka into a T responses.
        /// </summary>
        /// <param name="context">Context of the request.</param>
        /// <param name="payload">Buffer data returned by Kafka servers.</param>
        /// <returns></returns>
        T Decode(byte[] payload);
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