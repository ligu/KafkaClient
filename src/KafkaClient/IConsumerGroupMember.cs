using System;
using KafkaClient.Protocol;
using KafkaClient.Protocol.Types;

namespace KafkaClient
{
    public interface IConsumerGroupMember : IGroupMember, IDisposable
    {
        string LeaderId { get; }

        int GenerationId { get; }

        // is this necessary?
        IProtocolTypeEncoder Encoder { get; }
    }
}