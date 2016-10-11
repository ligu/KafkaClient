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
        public byte[] EncodeMetadata(IMemberMetadata metadata)
        {
            return EncodeMetadata((TMetadata)metadata);
        }

        /// <inheritdoc />
        public byte[] EncodeAssignment(IMemberAssignment assignment)
        {
            return EncodeAssignment((TAssignment)assignment);
        }

        /// <inheritdoc />
        public abstract IMemberMetadata DecodeMetadata(byte[] bytes);

        /// <inheritdoc />
        public abstract IMemberAssignment DecodeAssignment(byte[] bytes);

        protected abstract byte[] EncodeMetadata(TMetadata metadata);
        protected abstract byte[] EncodeAssignment(TAssignment assignment);
    }

    public class ProtocolTypeEncoder : ProtocolTypeEncoder<ByteMember, ByteMember>
    {
        /// <inheritdoc />
        public ProtocolTypeEncoder() : base("")
        {
        }

        /// <inheritdoc />
        public override IMemberMetadata DecodeMetadata(byte[] bytes)
        {
            return new ByteMember(bytes);
        }

        /// <inheritdoc />
        public override IMemberAssignment DecodeAssignment(byte[] bytes)
        {
            return new ByteMember(bytes);
        }

        /// <inheritdoc />
        protected override byte[] EncodeMetadata(ByteMember metadata)
        {
            return metadata.Bytes;
        }

        /// <inheritdoc />
        protected override byte[] EncodeAssignment(ByteMember assignment)
        {
            return assignment.Bytes;
        }
    }
}