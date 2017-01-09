using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Protocol;

namespace KafkaClient
{
    public interface IConsumerGroupMember : IGroupMember, IAsyncDisposable
    {
        int GenerationId { get; }
        bool IsLeader { get; }
        string ProtocolType { get; }

        /// <summary>
        /// Called from the consumer, in the Joining state.
        /// 
        /// See https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Client-side+Assignment+Proposal
        /// </summary>
        void OnJoinGroup(JoinGroupResponse response);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="maxCount"></param>
        /// <param name="cancellationToken"></param>
        /// <returns><see cref="MeMessageBatch.Empty> if no more topic/partitions are available currently.</returns>
        Task<IConsumerMessageBatch> FetchBatchAsync(int maxCount, CancellationToken cancellationToken);
    }
}