using System;
using System.Collections.Immutable;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// LeaveGroupResponse => ErrorCode
    ///   ErrorCode => int16
    /// 
    /// see http://kafka.apache.org/protocol.html#protocol_messages
    /// </summary>
    public class LeaveGroupResponse : IResponse, IEquatable<LeaveGroupResponse>
    {
        public LeaveGroupResponse(ErrorResponseCode error)
        {
            Error = error;
            Errors = ImmutableList<ErrorResponseCode>.Empty.Add(Error);
        }

        /// <inheritdoc />
        public IImmutableList<ErrorResponseCode> Errors { get; }

        public ErrorResponseCode Error { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as LeaveGroupResponse);
        }

        /// <inheritdoc />
        public bool Equals(LeaveGroupResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Error == other.Error;
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return (int) Error;
        }

        /// <inheritdoc />
        public static bool operator ==(LeaveGroupResponse left, LeaveGroupResponse right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(LeaveGroupResponse left, LeaveGroupResponse right)
        {
            return !Equals(left, right);
        }
    }
}