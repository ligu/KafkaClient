using System.Collections.Immutable;

namespace KafkaNet.Protocol
{
    public interface IKafkaResponse
    {
        /// <summary>
        /// Any errors from the server
        /// </summary>
        ImmutableList<ErrorResponseCode> Errors { get; }
    }
}