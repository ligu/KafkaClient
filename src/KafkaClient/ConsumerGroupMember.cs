using System;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Protocol.Types;
using Nito.AsyncEx;

namespace KafkaClient
{
    public class ConsumerGroupMember : IConsumerGroupMember
    {
        private readonly IConsumer _consumer;

        public ConsumerGroupMember(IConsumer consumer, string groupId, string memberId, string leaderId, int generationId, IProtocolTypeEncoder encoder)
        {
            _consumer = consumer;
            GroupId = groupId;
            MemberId = memberId;
            LeaderId = leaderId;
            GenerationId = generationId;
            Encoder = encoder;
        }

        private int _leaveGroupCount = 0;

        public string GroupId { get; }
        public string MemberId { get; }

        public string LeaderId { get; }
        public int GenerationId { get; }
        public IProtocolTypeEncoder Encoder { get; }

        /// <summary>
        /// Leave the consumer group.
        /// </summary>
        public async Task<bool> LeaveGroupAsync(CancellationToken cancellationToken)
        {
            if (Interlocked.Increment(ref _leaveGroupCount) != 1) return false;

            StopHeartbeat();
            await _consumer.LeaveConsumerGroupAsync(GroupId, MemberId, cancellationToken);
            return true;
        }

        private void StopHeartbeat()
        {
            
        }

        public void Dispose()
        {
            StopHeartbeat();
            AsyncContext.Run(() => _consumer.LeaveConsumerGroupAsync(GroupId, MemberId, CancellationToken.None, false));
        }
    }
}