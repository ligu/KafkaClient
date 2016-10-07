using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// Buffer represents a collection of messages to be posted to a specified Topic on specified Partition.
    /// Included in <see cref="ProduceRequest"/>
    /// </summary>
    public class Payload : Topic
    {
        public Payload(string topicName, int partitionId, IEnumerable<Message> messages, MessageCodec codec = MessageCodec.CodecNone) 
            : base(topicName, partitionId)
        {
            Codec = codec;
            Messages = ImmutableList<Message>.Empty.AddNotNullRange(messages);
        }

        public MessageCodec Codec { get; }
        public IImmutableList<Message> Messages { get; }
    }
}