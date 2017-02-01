using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// ProduceRequest => RequiredAcks Timeout [TopicData]
    ///  RequiredAcks => int16   -- This field indicates how many acknowledgements the servers should receive before responding to the request. 
    ///                             If it is 0 the server will not send any response (this is the only case where the server will not reply to 
    ///                             a request). If it is 1, the server will wait the data is written to the local log before sending a response. 
    ///                             If it is -1 the server will block until the message is committed by all in sync replicas before sending a response.
    ///  Timeout => int32        -- This provides a maximum time in milliseconds the server can await the receipt of the number of acknowledgements 
    ///                             in RequiredAcks. The timeout is not an exact limit on the request time for a few reasons: (1) it does not include 
    ///                             network latency, (2) the timer begins at the beginning of the processing of this request so if many requests are 
    ///                             queued due to server overload that wait time will not be included, (3) we will not terminate a local write so if 
    ///                             the local write time exceeds this timeout it will not be respected. To get a hard timeout of this type the client 
    ///                             should use the socket timeout.
    ///  TopicData => TopicName Data
    ///   TopicName => string    -- The topic that data is being published to.
    ///   Data => Partition MessageSet
    ///    Partition => int32    -- The partition that data is being published to.
    ///    MessageSet => BYTES   -- The size (and bytes) of the message set that follows.
    /// 
    /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets
    /// </summary>
    public class ProduceRequest : Request, IRequest<ProduceResponse>, IEquatable<ProduceRequest>
    {
        public override string ToString() => $"{{Api:{ApiKey},RequiredAcks:{Acks},Timeout:{Timeout},Topics:[{Topics.ToStrings()}]}}";

        public override string ShortString() => Topics.Count == 1 ? $"{ApiKey} {Topics[0].TopicName}" : ApiKey.ToString();

        public ProduceRequest(Topic topic, TimeSpan? timeout = null, short acks = 1)
            : this(new [] { topic }, timeout, acks)
        {
        }

        public ProduceRequest(IEnumerable<Topic> payload, TimeSpan? timeout = null, short acks = 1) 
            : base(Protocol.ApiKey.Produce, acks != 0)
        {
            Timeout = timeout.GetValueOrDefault(TimeSpan.FromSeconds(1));
            Acks = acks;
            Topics = payload != null ? payload.ToImmutableList() : ImmutableList<Topic>.Empty;
        }

        /// <summary>
        /// Time kafka will wait for requested ack level before returning.
        /// </summary>
        public TimeSpan Timeout { get; }

        /// <summary>
        /// Level of ack required by kafka: 0 immediate, 1 written to leader, 2+ replicas synced, -1 all replicas
        /// </summary>
        public short Acks { get; }

        /// <summary>
        /// Collection of payloads to post to kafka
        /// </summary>
        public IImmutableList<Topic> Topics { get; }

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
            return Timeout.Equals(other.Timeout) 
                && Acks == other.Acks 
                && Topics.HasEqualElementsInOrder(other.Topics);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = Timeout.GetHashCode();
                hashCode = (hashCode*397) ^ Acks.GetHashCode();
                hashCode = (hashCode*397) ^ (Topics?.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        /// <inheritdoc />
        public static bool operator ==(ProduceRequest left, ProduceRequest right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(ProduceRequest left, ProduceRequest right)
        {
            return !Equals(left, right);
        }

        #endregion

        /// <summary>
        /// Buffer represents a collection of messages to be posted to a specified Topic on specified Partition.
        /// Included in <see cref="ProduceRequest"/>
        /// </summary>
        public class Topic : TopicPartition, IEquatable<Topic>
        {
            public override string ToString() => $"{{TopicName:{TopicName},PartitionId:{PartitionId},Codec:{Codec},Messages:{Messages.Count}}}";

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
                    hashCode = (hashCode*397) ^ (Messages?.GetHashCode() ?? 0);
                    return hashCode;
                }
            }

            /// <inheritdoc />
            public static bool operator ==(Topic left, Topic right)
            {
                return Equals(left, right);
            }

            /// <inheritdoc />
            public static bool operator !=(Topic left, Topic right)
            {
                return !Equals(left, right);
            }

            #endregion

        }
    }
}