using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol.Types
{
    public abstract class ProtocolTypeEncoder<TMetadata, TAssignment> : IProtocolTypeEncoder
        where TMetadata : IMemberMetadata
        where TAssignment : IMemberAssignment
    {
        protected ProtocolTypeEncoder(string type)
        {
            Type = type;
        }

        /// <inheritdoc />
        public string Type { get; }

        /// <inheritdoc />
        public void EncodeMetadata(IKafkaWriter writer, IMemberMetadata value)
        {
            using (writer.MarkForLength()) {
                EncodeMetadata(writer, (TMetadata) value);
            }
        }

        /// <inheritdoc />
        public void EncodeAssignment(IKafkaWriter writer, IMemberAssignment value)
        {
            using (writer.MarkForLength()) {
                EncodeAssignment(writer, (TAssignment) value);
            }
        }

        /// <inheritdoc />
        public IMemberMetadata DecodeMetadata(IKafkaReader reader)
        {
            var expectedLength = reader.ReadInt32();
            if (!reader.Available(expectedLength)) throw new BufferUnderRunException($"{Type} Metadata size of {expectedLength} is not fully available.");
            
            return DecodeMetadata(reader, expectedLength);
        }

        /// <inheritdoc />
        public IMemberAssignment DecodeAssignment(IKafkaReader reader)
        {
            var expectedLength = reader.ReadInt32();
            if (!reader.Available(expectedLength)) throw new BufferUnderRunException($"{Type} Assignment size of {expectedLength} is not fully available.");
            
            return DecodeAssignment(reader, expectedLength);
        }

        protected abstract void EncodeMetadata(IKafkaWriter writer, TMetadata value);
        protected abstract void EncodeAssignment(IKafkaWriter writer, TAssignment value);
        protected abstract TMetadata DecodeMetadata(IKafkaReader reader, int expectedLength);
        protected abstract TAssignment DecodeAssignment(IKafkaReader reader, int expectedLength);

        public IImmutableDictionary<string, IMemberAssignment> AssignMembers(IImmutableDictionary<string, IMemberMetadata> memberMetadata)
        {
            throw new System.NotImplementedException();
        }
    }

    public class ProtocolTypeEncoder : ProtocolTypeEncoder<ByteMemberMetadata, ByteMemberAssignment>
    {
        /// <inheritdoc />
        public ProtocolTypeEncoder(string type = "") : base(type)
        {
        }

        /// <inheritdoc />
        protected override void EncodeMetadata(IKafkaWriter writer, ByteMemberMetadata value)
        {
            writer.Write(value.Bytes);
        }

        /// <inheritdoc />
        protected override void EncodeAssignment(IKafkaWriter writer, ByteMemberAssignment value)
        {
            writer.Write(value.Bytes);
        }

        protected override ByteMemberMetadata DecodeMetadata(IKafkaReader reader, int expectedLength)
        {
            return new ByteMemberMetadata(Type, reader.ReadBytes());
        }

        protected override ByteMemberAssignment DecodeAssignment(IKafkaReader reader, int expectedLength)
        {
            return new ByteMemberAssignment(reader.ReadBytes());
        }
    }
}