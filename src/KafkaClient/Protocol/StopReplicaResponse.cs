using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// StopReplica Response => error_code [topic partition error_code] 
    ///   error_code => INT16 -- Error code.
    ///   topic => STRING     -- Topic name.
    ///   partition => INT32  -- Topic partition id.
    ///   error_code => INT16 -- Error code.
    /// </summary>
    public class StopReplicaResponse : IResponse, IEquatable<StopReplicaResponse>
    {
        public StopReplicaResponse(ErrorResponseCode errorCode, IEnumerable<TopicResponse> topics)
        {
            ErrorCode = errorCode;
            Topics = ImmutableList<TopicResponse>.Empty.AddNotNullRange(topics);
            Errors = ImmutableList<ErrorResponseCode>.Empty.Add(errorCode).AddRange(Topics.Select(t => t.ErrorCode));
        }

        public ErrorResponseCode ErrorCode { get; }

        /// <inheritdoc />
        public IImmutableList<ErrorResponseCode> Errors { get; }

        public IImmutableList<TopicResponse> Topics { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as StopReplicaResponse);
        }

        /// <inheritdoc />
        public bool Equals(StopReplicaResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return ErrorCode == other.ErrorCode 
                   && Topics.HasEqualElementsInOrder(other.Topics);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                return ((int) ErrorCode*397) ^ (Topics?.GetHashCode() ?? 0);
            }
        }

        /// <inheritdoc />
        public static bool operator ==(StopReplicaResponse left, StopReplicaResponse right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(StopReplicaResponse left, StopReplicaResponse right)
        {
            return !Equals(left, right);
        }
    }
}