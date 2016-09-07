using System.Collections.Immutable;

namespace KafkaNet.Protocol
{
    public class GroupCoordinatorResponse : Broker, IKafkaResponse
    {
        public GroupCoordinatorResponse(int correlationId, ErrorResponseCode errorCode, int coordinatorId, string host, int port)
            : base(coordinatorId, host, port)
        {
            CorrelationId = correlationId;
            Error = errorCode;
            Errors = ImmutableList<ErrorResponseCode>.Empty.Add(Error);
        }

        /// <summary>
        /// Error code of exception that occured during the request.  Zero if no error.
        /// </summary>
        public ErrorResponseCode Error { get; }

        public int CorrelationId { get; }

        public ImmutableList<ErrorResponseCode> Errors { get; }
    }
}