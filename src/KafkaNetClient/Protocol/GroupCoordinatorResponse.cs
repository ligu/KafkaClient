using System.Collections.Immutable;

namespace KafkaNet.Protocol
{
    public class GroupCoordinatorResponse : Broker, IKafkaResponse
    {
        public GroupCoordinatorResponse(int correlationId, ErrorResponseCode errorCode, int coordinatorId, string host, int port)
            : base(coordinatorId, host, port)
        {
            CorrelationId = correlationId;
            ErrorCode = errorCode;
            Errors = ImmutableList<ErrorResponseCode>.Empty.Add(ErrorCode);
        }

        /// <summary>
        /// Error code of exception that occured during the request.  Zero if no error.
        /// </summary>
        public ErrorResponseCode ErrorCode { get; }

        public int CorrelationId { get; }

        public ImmutableList<ErrorResponseCode> Errors { get; }
    }
}