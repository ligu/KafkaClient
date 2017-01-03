using System.Collections.Immutable;

namespace KafkaClient.Protocol.Types
{
    public interface ITypeAssigner
    {
        IImmutableDictionary<string, IMemberAssignment> AssignMembers(IImmutableDictionary<string, IMemberMetadata> memberMetadata);
    }
}