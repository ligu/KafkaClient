namespace KafkaClient.Protocol.Types
{
    public interface IMemberMetadata
    {
        string ProtocolType { get; }
        string AssignmentStrategy { get; }
    }
}