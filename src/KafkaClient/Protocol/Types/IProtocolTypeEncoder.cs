using KafkaClient.Common;

namespace KafkaClient.Protocol.Types
{
    public interface IProtocolTypeEncoder
    {
        string Type { get; }

        void EncodeMetadata(IKafkaWriter writer, IMemberMetadata value);
        void EncodeAssignment(IKafkaWriter writer, IMemberAssignment value);

        IMemberMetadata DecodeMetadata(IKafkaReader reader);
        IMemberAssignment DecodeAssignment(IKafkaReader reader);
    }
}