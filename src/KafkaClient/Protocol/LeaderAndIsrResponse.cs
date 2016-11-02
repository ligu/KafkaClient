using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// LeaderAndIsrResponse => error_code [partitions] 
    ///  error_code => INT16
    ///  partitions => topic partition error_code 
    ///    topic => STRING
    ///    partition => INT32
    ///    error_code => INT16
    /// </summary>
    public class LeaderAndIsrResponse : IResponse, IEquatable<LeaderAndIsrResponse>
    {
        public LeaderAndIsrResponse(ErrorResponseCode errorCode, IEnumerable<TopicResponse> topics = null)
        {
            ErrorCode = errorCode;
            Topics = ImmutableList<TopicResponse>.Empty.AddNotNullRange(topics);
            Errors = ImmutableList<ErrorResponseCode>.Empty.Add(errorCode);
        }

        public ErrorResponseCode ErrorCode { get; }
        public IImmutableList<ErrorResponseCode> Errors { get; }

        public IImmutableList<TopicResponse> Topics { get; }

        public override bool Equals(object obj)
        {
            return Equals(obj as LeaderAndIsrResponse);
        }

        public bool Equals(LeaderAndIsrResponse other)
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

        public static bool operator ==(LeaderAndIsrResponse left, LeaderAndIsrResponse right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(LeaderAndIsrResponse left, LeaderAndIsrResponse right)
        {
            return !Equals(left, right);
        }
    }
}