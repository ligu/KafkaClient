using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// ProduceRequest => acks timeout [topics]
    ///  acks => INT16           -- This field indicates how many acknowledgements the servers should receive before responding to the request. 
    ///                             If it is 0 the server will not send any response (this is the only case where the server will not reply to 
    ///                             a request). If it is 1, the server will wait the data is written to the local log before sending a response. 
    ///                             If it is -1 the server will block until the message is committed by all in sync replicas before sending a response.
    ///  timeout => INT32        -- This provides a maximum time in milliseconds the server can await the receipt of the number of acknowledgements 
    ///                             in RequiredAcks. The timeout is not an exact limit on the request time for a few reasons: (1) it does not include 
    ///                             network latency, (2) the timer begins at the beginning of the processing of this request so if many requests are 
    ///                             queued due to server overload that wait time will not be included, (3) we will not terminate a local write so if 
    ///                             the local write time exceeds this timeout it will not be respected. To get a hard timeout of this type the client 
    ///                             should use the socket timeout.
    ///  topics => topic [partitions]
    ///   topic => STRING        -- The topic that data is being published to.
    ///   partitions => partition_id record_set
    ///    partition_id => INT32 -- The partition that data is being published to.
    ///    record_set => BYTES   -- The size (and bytes) of the message set that follows.
    /// 
    /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets
    /// </summary>
    public class ProduceRequest : Request, IRequest<ProduceResponse>, IEquatable<ProduceRequest>
    {
        public override string ToString() => $"{{Api:{ApiKey},acks:{acks},timeout:{timeout},topics:[{topics.ToStrings()}]}}";

        public override string ShortString() => topics.Count == 1 ? $"{ApiKey} {topics[0].topic}" : ApiKey.ToString();

        protected override void EncodeBody(IKafkaWriter writer, IRequestContext context)
        {
            var totalCompressedBytes = 0;
            var groupedPayloads = (from p in topics
                                   group p by new {
                                       p.topic, p.partition_id, p.Codec
                                   } into tpc
                                   select tpc).ToList();

            writer.Write(acks)
                    .Write((int)timeout.TotalMilliseconds)
                    .Write(groupedPayloads.Count);

            foreach (var groupedPayload in groupedPayloads) {
                var payloads = groupedPayload.ToList();
                writer.Write(groupedPayload.Key.topic)
                        .Write(payloads.Count)
                        .Write(groupedPayload.Key.partition_id);

                var compressedBytes = writer.Write(payloads.SelectMany(x => x.Messages), groupedPayload.Key.Codec);
                Interlocked.Add(ref totalCompressedBytes, compressedBytes);
            }

            if (context.OnProduceRequestMessages != null) {
                var segment = writer.ToSegment();
                context.OnProduceRequestMessages(topics.Sum(_ => _.Messages.Count), segment.Count, totalCompressedBytes);
            }
        }

        public ProduceRequest(Topic topic, TimeSpan? timeout = null, short acks = 1)
            : this(new [] { topic }, timeout, acks)
        {
        }

        public ProduceRequest(IEnumerable<Topic> payload, TimeSpan? timeout = null, short acks = 1) 
            : base(ApiKey.Produce, acks != 0)
        {
            this.timeout = timeout.GetValueOrDefault(TimeSpan.FromSeconds(1));
            this.acks = acks;
            topics = payload != null ? payload.ToImmutableList() : ImmutableList<Topic>.Empty;
        }

        /// <summary>
        /// Time kafka will wait for requested ack level before returning.
        /// </summary>
        public TimeSpan timeout { get; }

        /// <summary>
        /// Level of ack required by kafka: 0 immediate, 1 written to leader, 2+ replicas synced, -1 all replicas
        /// </summary>
        public short acks { get; }

        /// <summary>
        /// Collection of payloads to post to kafka
        /// </summary>
        public IImmutableList<Topic> topics { get; }

        #region Equality 

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as ProduceRequest);
        }

        /// <inheritdoc />
        public bool Equals(ProduceRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return timeout.Equals(other.timeout) 
                && acks == other.acks 
                && topics.HasEqualElementsInOrder(other.topics);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = timeout.GetHashCode();
                hashCode = (hashCode*397) ^ acks.GetHashCode();
                hashCode = (hashCode*397) ^ (topics?.Count.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        #endregion

        /// <summary>
        /// Buffer represents a collection of messages to be posted to a specified Topic on specified Partition.
        /// Included in <see cref="ProduceRequest"/>
        /// </summary>
        public class Topic : TopicPartition, IEquatable<Topic>
        {
            public override string ToString() => $"{{topic:{topic},partition_id:{partition_id},Codec:{Codec},Messages:{Messages.Count}}}";

            public Topic(string topicName, int partitionId, IEnumerable<Message> messages, MessageCodec codec = MessageCodec.None) 
                : base(topicName, partitionId)
            {
                Codec = codec;
                Messages = ImmutableList<Message>.Empty.AddNotNullRange(messages);
            }

            public MessageCodec Codec { get; }
            public IImmutableList<Message> Messages { get; }

            #region Equality

            /// <inheritdoc />
            public override bool Equals(object obj)
            {
                return Equals(obj as Topic);
            }

            /// <inheritdoc />
            public bool Equals(Topic other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return base.Equals(other) 
                    && Codec == other.Codec 
                    && Messages.HasEqualElementsInOrder(other.Messages);
            }

            /// <inheritdoc />
            public override int GetHashCode()
            {
                unchecked {
                    int hashCode = base.GetHashCode();
                    hashCode = (hashCode*397) ^ (int) Codec;
                    hashCode = (hashCode*397) ^ (Messages?.Count.GetHashCode() ?? 0);
                    return hashCode;
                }
            }

            #endregion

        }
    }
}