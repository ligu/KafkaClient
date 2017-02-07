using System;

namespace KafkaClient.Protocol
{
    public interface IRequest<out T> : IRequest
        where T : IResponse
    {
        /// <summary>
        /// Consume encoded format of the kafka response, received over tcp. Convert to the response object.
        /// </summary>
        T ToResponse(IRequestContext context, ArraySegment<byte> bytes);
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

        /// <summary>
        /// Produce Encoded format of the kafka request, to be sent over tcp. Includes the request header
        /// </summary>
        ArraySegment<byte> ToBytes(IRequestContext context);
    }
}