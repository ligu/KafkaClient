using System;
using KafkaClient.Protocol.Types;

namespace KafkaClient
{
    public class ConsumerGroupMember : IConsumerGroupMember
    {
        private readonly IConsumer _consumer;

        public ConsumerGroupMember(IConsumer consumer, string groupId, string memberId, string leaderId, int generationId)
        {
            _consumer = consumer;
            GroupId = groupId;
            MemberId = memberId;
            LeaderId = leaderId;
            GenerationId = generationId;

        }

        public string GroupId { get; }
        public string MemberId { get; }

        public void Dispose()
        {
            // on dispose, should leave group (so it doesn't have to wait for next heartbeat to fail
            throw new NotImplementedException();
        }

        public string LeaderId { get; }
        public int GenerationId { get; }
        public IProtocolTypeEncoder Encoder { get; }
    }
}