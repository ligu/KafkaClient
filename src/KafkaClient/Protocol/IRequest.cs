using System.Diagnostics.CodeAnalysis;

namespace KafkaClient.Protocol
{
    [SuppressMessage("ReSharper", "UnusedTypeParameter")]
    public interface IRequest<T> : IRequest
        where T : IResponse
    {
    }

    public interface IRequest
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