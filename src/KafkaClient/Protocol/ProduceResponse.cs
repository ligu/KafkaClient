using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;
// ReSharper disable InconsistentNaming

namespace KafkaClient.Protocol
{
    /// <summary>
    /// Produce Response => [response] *throttle_time_ms
    ///  *throttle_time_ms is only version 1 (0.9.0) and above
    /// 
    ///  response => topic [partition_responses]
    ///   topic => STRING          -- The topic this response entry corresponds to.
    /// 
    ///   partition_responses => partition_id error_code base_offset *timestamp 
    ///    *Timestamp is only version 2 (0.10.0) and above
    ///    partition_id => INT32   -- The partition this response entry corresponds to.
    ///    error_code => INT16     -- The error from this partition, if any. Errors are given on a per-partition basis because a given partition may be 
    ///                               unavailable or maintained on a different host, while others may have successfully accepted the produce request.
    ///    base_offset => INT64    -- The offset assigned to the first message in the message set appended to this partition.
    ///    timestamp => INT64      -- If LogAppendTime is used for the topic, this is the timestamp assigned by the broker to the message set. 
    ///                               All the messages in the message set have the same timestamp.
    ///                               If CreateTime is used, this field is always -1. The producer can assume the timestamp of the messages in the 
    ///                               produce request has been accepted by the broker if there is no error code returned.
    ///                               Unit is milliseconds since beginning of the epoch (midnight Jan 1, 1970 (UTC)).
    ///  throttle_time_ms => INT32 -- Duration in milliseconds for which the request was throttled due to quota violation. 
    ///                               (Zero if the request did not violate any quota).
    /// 
    /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets
    /// </summary>
    public class ProduceResponse : IResponse, IEquatable<ProduceResponse>
    {
        public override string ToString() => $"{{responses:[{responses.ToStrings()}],throttle_time_ms:{throttle_time_ms}}}";

        public static ProduceResponse FromBytes(IRequestContext context, ArraySegment<byte> bytes)
        {
            using (var reader = new KafkaReader(bytes)) {
                TimeSpan? throttleTime = null;

                var topics = new List<ProduceResponse.Topic>();
                var topicCount = reader.ReadInt32();
                for (var i = 0; i < topicCount; i++) {
                    var topicName = reader.ReadString();

                    var partitionCount = reader.ReadInt32();
                    for (var j = 0; j < partitionCount; j++) {
                        var partitionId = reader.ReadInt32();
                        var errorCode = (ErrorCode) reader.ReadInt16();
                        var offset = reader.ReadInt64();
                        DateTimeOffset? timestamp = null;

                        if (context.ApiVersion >= 2) {
                            var milliseconds = reader.ReadInt64();
                            if (milliseconds >= 0) {
                                timestamp = DateTimeOffset.FromUnixTimeMilliseconds(milliseconds);
                            }
                        }

                        topics.Add(new Topic(topicName, partitionId, errorCode, offset, timestamp));
                    }
                }

                if (context.ApiVersion >= 1) {
                    throttleTime = TimeSpan.FromMilliseconds(reader.ReadInt32());
                }
                return new ProduceResponse(topics, throttleTime);
            }
        }

        public ProduceResponse(Topic topic, TimeSpan? throttleTime = null)
            : this (new []{ topic }, throttleTime)
        {
        }

        public ProduceResponse(IEnumerable<Topic> topics = null, TimeSpan? throttleTime = null)
        {
            responses = ImmutableList<Topic>.Empty.AddNotNullRange(topics);
            Errors = ImmutableList<ErrorCode>.Empty.AddRange(responses.Select(t => t.error_code));
            throttle_time_ms = throttleTime;
        }

        public IImmutableList<ErrorCode> Errors { get; }

        public IImmutableList<Topic> responses { get; }

        /// <summary>
        /// Duration in milliseconds for which the request was throttled due to quota violation. 
        /// (Zero if the request did not violate any quota). Only version 1 (0.9.0) and above.
        /// </summary>
        public TimeSpan? throttle_time_ms { get; }

        #region Equality

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
            return responses.HasEqualElementsInOrder(other.responses) 
                && (int?)throttle_time_ms?.TotalMilliseconds == (int?)other.throttle_time_ms?.TotalMilliseconds;
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = responses?.Count.GetHashCode() ?? 0;
                hashCode = (hashCode*397) ^ throttle_time_ms.GetHashCode();
                return hashCode;
            }
        }

        #endregion

        public class Topic : TopicResponse, IEquatable<Topic>
        {
            public override string ToString() => $"{{topic:{topic},partition_id:{partition_id},error_code:{error_code},base_offset:{base_offset},timestamp:{timestamp}}}";

            public Topic(string topic, int partitionId, ErrorCode errorCode, long offset, DateTimeOffset? timestamp = null)
                : base(topic, partitionId, errorCode)
            {
                base_offset = offset;
                this.timestamp = timestamp.HasValue && timestamp.Value.ToUnixTimeMilliseconds() >= 0 ? timestamp : null;
            }

            /// <summary>
            /// The offset number to commit as completed.
            /// </summary>
            public long base_offset { get; }

            /// <summary>
            /// If LogAppendTime is used for the topic, this is the timestamp assigned by the broker to the message set. 
            /// All the messages in the message set have the same timestamp.
            /// If CreateTime is used, this field is always -1. The producer can assume the timestamp of the messages in the 
            /// produce request has been accepted by the broker if there is no error code returned.
            /// </summary>
            public DateTimeOffset? timestamp { get; }

            #region Equality

            public override bool Equals(object obj)
            {
                return Equals(obj as Topic);
            }

            public bool Equals(Topic other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return base.Equals(other) 
                    && base_offset == other.base_offset 
                    && timestamp.Equals(other.timestamp);
            }

            public override int GetHashCode()
            {
                unchecked {
                    int hashCode = base.GetHashCode();
                    hashCode = (hashCode*397) ^ base_offset.GetHashCode();
                    hashCode = (hashCode*397) ^ timestamp.GetHashCode();
                    return hashCode;
                }
            }

            #endregion
        }
    }
}