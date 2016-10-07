using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// ProduceResponse => [TopicName [Partition ErrorCode Offset *Timestamp]] *ThrottleTime
    ///  *ThrottleTime is only version 1 (0.9.0) and above
    ///  *Timestamp is only version 2 (0.10.0) and above
    ///  TopicName => string   -- The topic this response entry corresponds to.
    ///  Partition => int32    -- The partition this response entry corresponds to.
    ///  ErrorCode => int16    -- The error from this partition, if any. Errors are given on a per-partition basis because a given partition may be 
    ///                           unavailable or maintained on a different host, while others may have successfully accepted the produce request.
    ///  Offset => int64       -- The offset assigned to the first message in the message set appended to this partition.
    ///  Timestamp => int64    -- If LogAppendTime is used for the topic, this is the timestamp assigned by the broker to the message set. 
    ///                           All the messages in the message set have the same timestamp.
    ///                           If CreateTime is used, this field is always -1. The producer can assume the timestamp of the messages in the 
    ///                           produce request has been accepted by the broker if there is no error code returned.
    ///                           Unit is milliseconds since beginning of the epoch (midnight Jan 1, 1970 (UTC)).
    ///  ThrottleTime => int32 -- Duration in milliseconds for which the request was throttled due to quota violation. 
    ///                           (Zero if the request did not violate any quota).
    /// 
    /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets
    /// </summary>
    public class ProduceResponse : IResponse, IEquatable<ProduceResponse>
    {
        public ProduceResponse(ProduceTopic topic, TimeSpan? throttleTime = null)
            : this (new []{ topic }, throttleTime)
        {
        }

        public ProduceResponse(IEnumerable<ProduceTopic> topics = null, TimeSpan? throttleTime = null)
        {
            Topics = ImmutableList<ProduceTopic>.Empty.AddNotNullRange(topics);
            Errors = ImmutableList<ErrorResponseCode>.Empty.AddRange(Topics.Select(t => t.ErrorCode));
            ThrottleTime = throttleTime;
        }

        public IImmutableList<ErrorResponseCode> Errors { get; }

        public IImmutableList<ProduceTopic> Topics { get; }

        /// <summary>
        /// Duration in milliseconds for which the request was throttled due to quota violation. 
        /// (Zero if the request did not violate any quota). Only version 1 (0.9.0) and above.
        /// </summary>
        public TimeSpan? ThrottleTime { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as ProduceResponse);
        }

        /// <inheritdoc />
        public bool Equals(ProduceResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Topics.HasEqualElementsInOrder(other.Topics) 
                && (int?)ThrottleTime?.TotalMilliseconds == (int?)other.ThrottleTime?.TotalMilliseconds;
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = Topics?.GetHashCode() ?? 0;
                hashCode = (hashCode*397) ^ ThrottleTime.GetHashCode();
                return hashCode;
            }
        }

        /// <inheritdoc />
        public static bool operator ==(ProduceResponse left, ProduceResponse right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(ProduceResponse left, ProduceResponse right)
        {
            return !Equals(left, right);
        }
    }
}