using System.Collections.Immutable;

namespace KafkaClient.Protocol
{
    public class GroupCoordinatorResponse : Broker, IResponse
    {
        public GroupCoordinatorResponse(ErrorResponseCode errorCode, int coordinatorId, string host, int port)
            : base(coordinatorId, host, port)
        {
            ErrorCode = errorCode;
            Errors = ImmutableList<ErrorResponseCode>.Empty.Add(ErrorCode);
        }

        /// <summary>
        /// Error code of exception that occured during the request.  Zero if no error.
        /// </summary>
        public ErrorResponseCode ErrorCode { get; }

        public ImmutableList<ErrorResponseCode> Errors { get; }
    }
}