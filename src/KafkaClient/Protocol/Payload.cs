using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// Buffer represents a collection of messages to be posted to a specified Topic on specified Partition.
    /// Included in <see cref="ProduceRequest"/>
    /// </summary>
    public class Payload : Topic, IEquatable<Payload>
    {
        public Payload(string topicName, int partitionId, IEnumerable<Message> messages, MessageCodec codec = MessageCodec.CodecNone) 
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
            return Equals(obj as Payload);
        }

        /// <inheritdoc />
        public bool Equals(Payload other)
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
        public static bool operator ==(Payload left, Payload right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(Payload left, Payload right)
        {
            return !Equals(left, right);
        }

        #endregion

    }
}