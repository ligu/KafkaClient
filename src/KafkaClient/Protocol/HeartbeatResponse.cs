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
        public override string ToString() => $"{{ErrorCode:{ErrorCode}}}";

        public HeartbeatResponse(ErrorCode errorCode)
        {
            ErrorCode = errorCode;
            Errors = ImmutableList<ErrorCode>.Empty.Add(ErrorCode);
        }

        /// <inheritdoc />
        public IImmutableList<ErrorCode> Errors { get; }

        public ErrorCode ErrorCode { get; }

        #region Equality

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
            return ErrorCode == other.ErrorCode;
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return (int) ErrorCode;
        }

        #endregion
    }
}