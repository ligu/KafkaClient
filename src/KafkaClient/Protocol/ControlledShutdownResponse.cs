using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// ControlledShutdown Response (Version: 1) => error_code [partitions_remaining] 
    ///  error_code => INT16
    ///  partitions_remaining => topic partition 
    ///    topic => STRING
    ///    partition => INT32
    /// </summary>
    public class ControlledShutdownResponse : IResponse, IEquatable<ControlledShutdownResponse>
    {
        public ControlledShutdownResponse(ErrorResponseCode errorCode, IEnumerable<TopicPartition> topics = null)
        {
            ErrorCode = errorCode;
            Topics = ImmutableList<TopicPartition>.Empty.AddNotNullRange(topics);
            Errors = ImmutableList<ErrorResponseCode>.Empty.Add(errorCode);

        }

        public ErrorResponseCode ErrorCode { get; }
        public IImmutableList<ErrorResponseCode> Errors { get; }

        public IImmutableList<TopicPartition> Topics { get; }

        public override bool Equals(object obj)
        {
            return Equals(obj as ControlledShutdownResponse);
        }

        public bool Equals(ControlledShutdownResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return ErrorCode == other.ErrorCode 
                && Topics.HasEqualElementsInOrder(other.Topics);
        }

        public override int GetHashCode()
        {
            unchecked {
                var hashCode = (int) ErrorCode;
                hashCode = (hashCode * 397) ^ (Topics?.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        public static bool operator ==(ControlledShutdownResponse left, ControlledShutdownResponse right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(ControlledShutdownResponse left, ControlledShutdownResponse right)
        {
            return !Equals(left, right);
        }
    }
}