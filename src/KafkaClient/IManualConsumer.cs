using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Protocol;

namespace KafkaClient
{
    public interface IManualConsumer
    {
        Task UpdateOrCreateOffsetAsync(string consumerGroup, long offset, CancellationToken cancellationToken);

        Task<long> FetchLastOffsetAsync(CancellationToken cancellationToken);

        Task<long> FetchOffsetAsync(string consumerGroup, CancellationToken cancellationToken);

        Task<IEnumerable<Message>> FetchMessagesAsync(int maxCount, long offset, CancellationToken cancellationToken);
    }
}