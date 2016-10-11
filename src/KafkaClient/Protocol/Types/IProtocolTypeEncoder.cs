namespace KafkaClient.Protocol.Types
{
    public interface IProtocolTypeEncoder
    {
        string Type { get; }

        byte[] EncodeMetadata(IMemberMetadata metadata);
        byte[] EncodeAssignment(IMemberAssignment assignment);

        IMemberMetadata DecodeMetadata(byte[] bytes);
        IMemberAssignment DecodeAssignment(byte[] bytes);
    }
}