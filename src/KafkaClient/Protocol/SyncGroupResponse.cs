using System;
using System.Collections.Immutable;
using KafkaClient.Assignment;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// SyncGroupResponse => ErrorCode MemberAssignment
    ///   ErrorCode => int16
    ///   MemberAssignment => bytes
    /// 
    /// see https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
    /// </summary>
    public class SyncGroupResponse : IResponse, IEquatable<SyncGroupResponse>
    {
        public override string ToString() => $"{{ErrorCode:{ErrorCode},MemberAssignment:{MemberAssignment}}}";

        public SyncGroupResponse(ErrorCode errorCode, IMemberAssignment memberAssignment)
        {
            ErrorCode = errorCode;
            Errors = ImmutableList<ErrorCode>.Empty.Add(ErrorCode);
            MemberAssignment = memberAssignment;
        }

        /// <inheritdoc />
        public IImmutableList<ErrorCode> Errors { get; }

        public ErrorCode ErrorCode { get; }
        public IMemberAssignment MemberAssignment { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as SyncGroupResponse);
        }

        /// <inheritdoc />
        public bool Equals(SyncGroupResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return ErrorCode == other.ErrorCode 
                && Equals(MemberAssignment, other.MemberAssignment);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                return ((int) ErrorCode*397) ^ (MemberAssignment?.GetHashCode() ?? 0);
            }
        }
        
        #endregion
    }
}