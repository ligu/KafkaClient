using KafkaClient.Common;

namespace KafkaClient.Protocol.Types
{
    public interface ITypeEncoder
    {
        string ProtocolType { get; }

        void EncodeMetadata(IKafkaWriter writer, IMemberMetadata value);
        void EncodeAssignment(IKafkaWriter writer, IMemberAssignment value);

        IMemberMetadata DecodeMetadata(string protocol, IKafkaReader reader);
        IMemberAssignment DecodeAssignment(IKafkaReader reader);

        ITypeAssigner GetAssigner(string protocol);
    }
}