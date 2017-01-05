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

        void OnRejoin(JoinGroupResponse response);

        Task<IConsumerMessageBatch> FetchMessagesAsync(int maxCount, CancellationToken cancellationToken);
    }
}