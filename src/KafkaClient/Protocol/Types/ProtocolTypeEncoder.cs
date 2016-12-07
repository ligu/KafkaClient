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
            EncodeMetadata(writer, (TMetadata) value);
        }

        /// <inheritdoc />
        public void EncodeAssignment(IKafkaWriter writer, IMemberAssignment value)
        {
            EncodeAssignment(writer, (TAssignment) value);
        }

        /// <inheritdoc />
        public abstract IMemberMetadata DecodeMetadata(IKafkaReader reader);

        /// <inheritdoc />
        public abstract IMemberAssignment DecodeAssignment(IKafkaReader reader);

        protected abstract void EncodeMetadata(IKafkaWriter writer, TMetadata value);
        protected abstract void EncodeAssignment(IKafkaWriter writer, TAssignment value);
    }

    public class ProtocolTypeEncoder : ProtocolTypeEncoder<ByteMemberMetadata, ByteMemberAssignment>
    {
        /// <inheritdoc />
        public ProtocolTypeEncoder(string type = "") : base(type)
        {
        }

        /// <inheritdoc />
        public override IMemberMetadata DecodeMetadata(IKafkaReader reader)
        {
            return new ByteMemberMetadata(Type, reader.ReadBytes());
        }

        /// <inheritdoc />
        public override IMemberAssignment DecodeAssignment(IKafkaReader reader)
        {
            return new ByteMemberAssignment(reader.ReadBytes());
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
    }
}