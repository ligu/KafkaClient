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
        /// Fetch messages for this consumer group's current assignment.
        /// Messages are collected together in a batch per assignment.
        /// Each batch can be used to get available messages, commit offsets and get subsequent batches on the given topic/partition.
        /// Once the topic/partition is reassigned, the batch will be disposed.
        /// </summary>
        /// <param name="maxCount"></param>
        /// <param name="cancellationToken"></param>
        /// <returns><see cref="MessageBatch.Empty"> if no more topic/partitions are available currently.</returns>
        Task<IMessageBatch> FetchBatchAsync(int maxCount, CancellationToken cancellationToken);
    }
}