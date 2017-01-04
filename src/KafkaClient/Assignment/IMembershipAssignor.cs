using System.Collections.Immutable;

namespace KafkaClient.Assignment
{
    public interface IMembershipAssignor
    {
        string AssignmentStrategy { get; }

        IImmutableDictionary<string, IMemberAssignment> AssignMembers(IImmutableDictionary<string, IMemberMetadata> memberMetadata);
    }
}