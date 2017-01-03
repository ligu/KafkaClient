using System;
using System.Collections.Immutable;

namespace KafkaClient.Protocol.Types
{
    public class ConsumerAssigner : TypeAssigner<ConsumerProtocolMetadata, ConsumerMemberAssignment>
    {
        protected override IImmutableDictionary<string, ConsumerMemberAssignment> Assign(IImmutableDictionary<string, ConsumerProtocolMetadata> memberMetadata)
        {
            throw new NotImplementedException();
        }
    }
}