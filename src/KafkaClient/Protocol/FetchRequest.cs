using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// FetchRequest => replica_id max_wait_time min_bytes *max_bytes [topics]
    ///  *max_bytes is only version 3 (0.10.1) and above
    ///  replica_id => INT32    -- The replica id indicates the node id of the replica initiating this request. Normal client consumers should always 
    ///                            specify this as -1 as they have no node id. Other brokers set this to be their own node id. The value -2 is accepted 
    ///                            to allow a non-broker to issue fetch requests as if it were a replica broker for debugging purposes.
    ///  max_wait_time => INT32 -- The max wait time is the maximum amount of time in milliseconds to block waiting if insufficient data is available 
    ///                            at the time the request is issued.
    ///  min_bytes => INT32     -- This is the minimum number of bytes of messages that must be available to give a response. If the client sets this 
    ///                            to 0 the server will always respond immediately, however if there is no new data since their last request they will 
    ///                            just get back empty message sets. If this is set to 1, the server will respond as soon as at least one partition has 
    ///                            at least 1 byte of data or the specified timeout occurs. By setting higher values in combination with the timeout the 
    ///                            consumer can tune for throughput and trade a little additional latency for reading only large chunks of data (e.g. 
    ///                            setting MaxWaitTime to 100 ms and setting MinBytes to 64k would allow the server to wait up to 100ms to try to accumulate 
    ///                            64k of data before responding).
    ///  max_bytes => INT32     -- Maximum bytes to accumulate in the response. Note that this is not an absolute maximum, if the first message in the 
    ///                            first non-empty partition of the fetch is larger than this value, the message will still be returned to ensure that 
    ///                            progress can be made.
    /// 
    ///  topics => topic [partitions]
    ///   topic => STRING        -- The name of the topic.
    /// 
    ///   partitions => partition_id fetch_offset max_bytes
    ///    partition_id => INT32 -- The id of the partition the fetch is for.
    ///    fetch_offset => INT64 -- The offset to begin this fetch from.
    ///    max_bytes => INT32    -- The maximum bytes to include in the message set for this partition. This helps bound the size of the response.
    /// 
    /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-FetchAPI
    /// </summary>
    public class FetchRequest : Request, IRequest<FetchResponse>, IEquatable<FetchRequest>
    {
        public override string ToString() => $"{{Api:{ApiKey},max_wait_time:{max_wait_time},min_bytes:{min_bytes},max_bytes:{max_bytes},topics:[{topics.ToStrings()}]}}";

        public override string ShortString() => topics.Count == 1 ? $"{ApiKey} {topics[0].topic}" : ApiKey.ToString();

        protected override void EncodeBody(IKafkaWriter writer, IRequestContext context)
        {
            var topicGroups = topics.GroupBy(x => x.topic).ToList();
            writer.Write(-1) // replica_id -- see above
                    .Write((int)Math.Min(int.MaxValue, max_wait_time.TotalMilliseconds))
                    .Write(min_bytes);

            if (context.ApiVersion >= 3) {
                writer.Write(max_bytes);
            }

            writer.Write(topicGroups.Count);
            foreach (var topicGroup in topicGroups) {
                var partitions = topicGroup.GroupBy(x => x.partition_id).ToList();
                writer.Write(topicGroup.Key)
                        .Write(partitions.Count);

                foreach (var partition in partitions) {
                    foreach (var fetch in partition) {
                        writer.Write(partition.Key)
                                .Write(fetch.fetch_offset)
                                .Write(fetch.max_bytes);
                    }
                }
            }
        }

        public FetchRequest(Topic topic, TimeSpan? maxWaitTime = null, int? minBytes = null, int? maxBytes = null) 
            : this (new []{ topic }, maxWaitTime, minBytes, maxBytes)
        {
        }

        public FetchRequest(IEnumerable<Topic> fetches = null, TimeSpan? maxWaitTime = null, int? minBytes = null, int? maxBytes = null) 
            : base(ApiKey.Fetch)
        {
            max_wait_time = maxWaitTime ?? TimeSpan.FromMilliseconds(DefaultMaxBlockingWaitTime);
            min_bytes = minBytes.GetValueOrDefault(DefaultMinBlockingByteBufferSize);
            max_bytes = maxBytes.GetValueOrDefault(min_bytes);
            topics = ImmutableList<Topic>.Empty.AddNotNullRange(fetches);
        }

        internal const int DefaultMinBlockingByteBufferSize = 4096;
        internal const int DefaultBufferSize = DefaultMinBlockingByteBufferSize * 8;
        internal const int DefaultMaxBlockingWaitTime = 5000;

        /// <summary>
        /// The max wait time is the maximum amount of time to block waiting if insufficient data is available at the time the request is issued.
        /// </summary>
        public TimeSpan max_wait_time { get; }

        /// <summary>
        /// This is the minimum number of bytes of messages that must be available to give a response.
        /// If the client sets this to 0 the server will always respond immediately, however if there is no new data since their last request they will just get back empty message sets.
        /// If this is set to 1, the server will respond as soon as at least one partition has at least 1 byte of data or the specified timeout occurs.
        /// By setting higher values in combination with the timeout the consumer can tune for throughput and trade a little additional latency for reading only large chunks of data
        /// (e.g. setting MaxWaitTime to 100 ms and setting MinBytes to 64k would allow the server to wait up to 100ms to try to accumulate 64k of data before responding).
        /// </summary>
        public int min_bytes { get; }

        /// <summary>
        /// Maximum bytes to accumulate in the response. Note that this is not an absolute maximum, if the first message in the first non-empty partition of the fetch is larger than 
        /// this value, the message will still be returned to ensure that progress can be made.
        /// </summary>
        public int max_bytes { get; }

        public IImmutableList<Topic> topics { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as FetchRequest);
        }

        /// <inheritdoc />
        public bool Equals(FetchRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return max_wait_time.Equals(other.max_wait_time) 
                && min_bytes == other.min_bytes 
                && max_bytes == other.max_bytes 
                && topics.HasEqualElementsInOrder(other.topics);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = max_wait_time.GetHashCode();
                hashCode = (hashCode*397) ^ min_bytes;
                hashCode = (hashCode*397) ^ max_bytes;
                hashCode = (hashCode*397) ^ (topics?.Count.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        #endregion

        public class Topic : TopicPartition, IEquatable<Topic>
        {
            public override string ToString() => $"{{topic:{topic},partition_id:{partition_id},fetch_offset:{fetch_offset},max_bytes:{max_bytes}}}";

            public Topic(string topicName, int partitionId, long offset, int? maxBytes = null)
                : base(topicName, partitionId)
            {
                fetch_offset = offset;
                max_bytes = maxBytes.GetValueOrDefault(DefaultMinBlockingByteBufferSize * 8);
            }

            /// <summary>
            /// The offset to begin this fetch from.
            /// </summary>
            public long fetch_offset { get; }

            /// <summary>
            /// The maximum bytes to include in the message set for this partition. This helps bound the size of the response.
            /// </summary>
            public int max_bytes { get; }

            #region Equality

            public override bool Equals(object obj)
            {
                return Equals(obj as Topic);
            }

            public bool Equals(Topic other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return Equals((TopicPartition) other) 
                    && fetch_offset == other.fetch_offset 
                    && max_bytes == other.max_bytes;
            }

            public override int GetHashCode()
            {
                unchecked {
                    int hashCode = base.GetHashCode();
                    hashCode = (hashCode*397) ^ fetch_offset.GetHashCode();
                    hashCode = (hashCode*397) ^ max_bytes;
                    return hashCode;
                }
            }

            #endregion
        }
    }
}