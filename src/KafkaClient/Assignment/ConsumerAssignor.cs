using System;
using System.Collections.Immutable;

namespace KafkaClient.Assignment
{
    public class ConsumerAssignor : MembershipAssignor<ConsumerProtocolMetadata, ConsumerMemberAssignment>
    {
        public static ImmutableList<IMembershipAssignor> Assignors { get; } = ImmutableList<IMembershipAssignor>.Empty.Add(new ConsumerAssignor());

        public const string Strategy = "sticky";

        public ConsumerAssignor() : base(Strategy)
        {
        }

        protected override IImmutableDictionary<string, ConsumerMemberAssignment> Assign(IImmutableDictionary<string, ConsumerProtocolMetadata> memberMetadata)
        {
            throw new NotImplementedException();
        }
    }
}