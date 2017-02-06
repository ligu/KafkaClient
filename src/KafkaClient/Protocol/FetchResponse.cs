using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// FetchResponse => *ThrottleTime [TopicData]
    ///  *ThrottleTime is only version 1 (0.9.0) and above
    ///  ThrottleTime => int32        -- Duration in milliseconds for which the request was throttled due to quota violation. (Zero if the request did not 
    ///                                  violate any quota.)
    /// 
    ///  TopicData => TopicName [PartitionData]
    ///   TopicName => string          -- The topic this response entry corresponds to.
    /// 
    ///   PartitionData => Partition ErrorCode HighwaterMarkOffset MessageSet
    ///    Partition => int32           -- The partition this response entry corresponds to.
    ///    ErrorCode => int16           -- The error from this partition, if any. Errors are given on a per-partition basis because a given partition may 
    ///                                    be unavailable or maintained on a different host, while others may have successfully accepted the produce request.
    ///    HighwaterMarkOffset => int64 -- The offset at the end of the log for this partition. This can be used by the client to determine how many messages 
    ///                                    behind the end of the log they are.
    ///    MessageSet => BYTES   -- The size (and bytes) of the message set that follows.
    /// 
    /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-FetchResponse
    /// </summary>
    public class FetchResponse : IResponse, IEquatable<FetchResponse>
    {
        public override string ToString() => $"{{ThrottleTime:{ThrottleTime},Topics:[{Topics.ToStrings()}]}}";

        public FetchResponse(IEnumerable<Topic> topics = null, TimeSpan? throttleTime = null)
        {
            Topics = ImmutableList<Topic>.Empty.AddNotNullRange(topics);
            Errors = ImmutableList<ErrorCode>.Empty.AddRange(Topics.Select(t => t.ErrorCode));
            ThrottleTime = throttleTime;
        }

        public IImmutableList<ErrorCode> Errors { get; }

        public IImmutableList<Topic> Topics { get; }

        /// <summary>
        /// Duration in milliseconds for which the request was throttled due to quota violation. (Zero if the request did not 
        /// violate any quota.) Only version 1 and above (0.9.0)
        /// </summary>
        public TimeSpan? ThrottleTime { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as FetchResponse);
        }

        /// <inheritdoc />
        public bool Equals(FetchResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Topics.HasEqualElementsInOrder(other.Topics) 
                && ThrottleTime.Equals(other.ThrottleTime);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                return ((Topics?.Count.GetHashCode() ?? 0)*397) ^ ThrottleTime.GetHashCode();
            }
        }

        #endregion

        public class Topic : TopicResponse, IEquatable<Topic>
        {
            public override string ToString() => $"{{TopicName:{topic},PartitionId:{partition_id},ErrorCode:{ErrorCode},HighwaterMarkOffset:{HighWaterMark},Messages:{Messages.Count}}}";

            public Topic(string topic, int partitionId, long highWaterMark, ErrorCode errorCode = ErrorCode.NONE, IEnumerable<Message> messages = null)
                : base(topic, partitionId, errorCode)
            {
                HighWaterMark = highWaterMark;
                Messages = ImmutableList<Message>.Empty.AddNotNullRange(messages);
            }

            /// <summary>
            /// The offset at the end of the log for this partition. This can be used by the client to determine how many messages behind the end of the log they are.
            /// </summary>
            public long HighWaterMark { get; }

            public IImmutableList<Message> Messages { get; }

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
                    && HighWaterMark == other.HighWaterMark 
                    && Messages.HasEqualElementsInOrder(other.Messages);
            }

            public override int GetHashCode()
            {
                unchecked {
                    int hashCode = base.GetHashCode();
                    hashCode = (hashCode*397) ^ HighWaterMark.GetHashCode();
                    hashCode = (hashCode*397) ^ (Messages?.Count.GetHashCode() ?? 0);
                    return hashCode;
                }
            }

            #endregion
        }
    }
}