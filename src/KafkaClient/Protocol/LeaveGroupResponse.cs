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
        public override string ToString() => $"{{ErrorCode:{ErrorCode}}}";

        public LeaveGroupResponse(ErrorCode errorCode)
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
            return Equals(obj as LeaveGroupResponse);
        }

        /// <inheritdoc />
        public bool Equals(LeaveGroupResponse other)
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