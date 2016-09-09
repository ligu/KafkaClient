namespace KafkaNet.Protocol
{
    /// <summary>
    /// The interface for encoding the request to the binary format for Kafka, and decoding the resulting binary response from the Kafka server.
    /// </summary>
    /// <typeparam name="TResponse">The type of the KafkaResponse expected back from the request.</typeparam>
    /// <typeparam name="TRequest">The type of the KafkaRequest being sent to the server.</typeparam>
    public interface IKafkaSerializer<in TRequest, out TResponse>
        where TRequest : IKafkaRequest
        where TResponse : IKafkaResponse
    {
        /// <summary>
        /// Encode this request into the Kafka wire protocol.
        /// </summary>
        /// <returns>Byte[] representing the binary wire protocol of this request.</returns>
        KafkaDataPayload Encode(IRequestContext context, TRequest request);

        /// <summary>
        /// Decode a response payload from Kafka into a T responses.
        /// </summary>
        /// <param name="context">Context of the request.</param>
        /// <param name="payload">Buffer data returned by Kafka servers.</param>
        /// <returns></returns>
        TResponse Decode(IRequestContext context, byte[] payload);
    }
}