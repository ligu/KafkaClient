namespace KafkaClient.Protocol
{
    public interface IRequest<T> : IRequest
        where T : IResponse
    {
    }

    public interface IRequest
    {
        /// <summary>
        /// Indicates this request should wait for a response from the server
        /// </summary>
        bool ExpectResponse { get; }

        /// <summary>
        /// Enum identifying the specific type of request message being represented.
        /// </summary>
        ApiKey ApiKey { get; }

        /// <summary>
        /// Short version of ToString, for writing only the most relevant information to the logs
        /// </summary>
        string ShortString();
    }
}