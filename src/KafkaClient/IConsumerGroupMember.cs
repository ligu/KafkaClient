using System;
using KafkaClient.Protocol;

namespace KafkaClient
{
    public interface IConsumerGroupMember : IGroupMember, IDisposable
    {
        int GenerationId { get; }

        bool IsLeader { get; }

        void OnRejoin(JoinGroupResponse response);
    }
}