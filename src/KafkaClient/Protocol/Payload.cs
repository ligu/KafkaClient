using System.Collections.Generic;
using System.Collections.Immutable;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// Buffer represents a collection of messages to be posted to a specified Topic on specified Partition.
    /// </summary>
    public class Payload : Topic
    {
        public Payload(string topicName, int partitionId, IEnumerable<Message> messages, MessageCodec codec = MessageCodec.CodecNone) 
            : base(topicName, partitionId)
        {
            Codec = codec;
            Messages = messages != null ? ImmutableList<Message>.Empty.AddRange(messages) : ImmutableList<Message>.Empty;
        }

        public MessageCodec Codec { get; }
        public ImmutableList<Message> Messages { get; }
    }
}