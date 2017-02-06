using System;
using System.Collections.Immutable;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// GroupCoordinatorResponse => ErrorCode NodeId Host Port
    ///  ErrorCode => int16 -- The error code.
    ///  NodeId => int32    -- The broker id.
    ///  Host => string     -- The hostname of the broker.
    ///  Port => int32      -- The port on which the broker accepts requests.
    ///
    /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
    /// </summary>
    public class GroupCoordinatorResponse : Server, IResponse, IEquatable<GroupCoordinatorResponse>
    {
        public override string ToString() => $"{{ErrorCode:{ErrorCode},NodeId:{Id},Host:'{Host}',Port:{Port}}}";

        public GroupCoordinatorResponse(ErrorCode errorCode, int coordinatorId, string host, int port)
            : base(coordinatorId, host, port)
        {
            ErrorCode = errorCode;
            Errors = ImmutableList<ErrorCode>.Empty.Add(ErrorCode);
        }

        /// <summary>
        /// Error code of exception that occured during the request.  Zero if no error.
        /// </summary>
        public ErrorCode ErrorCode { get; }

        public IImmutableList<ErrorCode> Errors { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as GroupCoordinatorResponse);
        }

        /// <inheritdoc />
        public bool Equals(GroupCoordinatorResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) && ErrorCode == other.ErrorCode;
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                return (base.GetHashCode()*397) ^ (int) ErrorCode;
            }
        }
    }
}