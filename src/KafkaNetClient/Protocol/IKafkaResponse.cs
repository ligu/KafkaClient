using System.Collections.Generic;
using System.Collections.Immutable;

namespace KafkaNet.Protocol
{
    public interface IKafkaResponse
    {
        /// <summary>
        /// Request Correlation
        /// </summary>
        int CorrelationId { get; }

        /// <summary>
        /// Any errors from the server (that are != NoError)
        /// </summary>
        ImmutableList<ErrorResponseCode> Errors { get; }
    }
}