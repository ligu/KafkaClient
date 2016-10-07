using System;
using System.Collections.Immutable;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// HeartbeatResponse => ErrorCode
    ///   ErrorCode => int16
    /// 
    /// see http://kafka.apache.org/protocol.html#protocol_messages
    /// </summary>
    public class HeartbeatResponse : IResponse, IEquatable<HeartbeatResponse>
    {
        public HeartbeatResponse(ErrorResponseCode error)
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
            return Equals(obj as HeartbeatResponse);
        }

        /// <inheritdoc />
        public bool Equals(HeartbeatResponse other)
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
        public static bool operator ==(HeartbeatResponse left, HeartbeatResponse right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(HeartbeatResponse left, HeartbeatResponse right)
        {
            return !Equals(left, right);
        }
    }
}