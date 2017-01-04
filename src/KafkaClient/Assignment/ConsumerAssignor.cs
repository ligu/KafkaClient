using System;
using System.Collections.Immutable;

namespace KafkaClient.Assignment
{
    public class ConsumerAssignor : MembershipAssignor<ConsumerProtocolMetadata, ConsumerMemberAssignment>
    {
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