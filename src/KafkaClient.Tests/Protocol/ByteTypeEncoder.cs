using System;
using KafkaClient.Common;
using KafkaClient.Protocol.Types;

namespace KafkaClient.Tests.Protocol
{
    public class ByteTypeEncoder : TypeEncoder<ByteTypeMetadata, ByteTypeAssignment>
    {
        /// <inheritdoc />
        public ByteTypeEncoder(string type = "") : base(type)
        {
        }

        /// <inheritdoc />
        protected override void EncodeMetadata(IKafkaWriter writer, ByteTypeMetadata value)
        {
            writer.Write(value.Bytes);
        }

        /// <inheritdoc />
        protected override void EncodeAssignment(IKafkaWriter writer, ByteTypeAssignment value)
        {
            writer.Write(value.Bytes);
        }

        protected override ByteTypeMetadata DecodeMetadata(string assignmentStrategy, IKafkaReader reader, int expectedLength)
        {
            return new ByteTypeMetadata(ProtocolType, assignmentStrategy, reader.ReadBytes());
        }

        protected override ByteTypeAssignment DecodeAssignment(IKafkaReader reader, int expectedLength)
        {
            return new ByteTypeAssignment(reader.ReadBytes());
        }

        public override ITypeAssigner GetAssigner(string protocol)
        {
            throw new NotImplementedException();
        }
    }
}