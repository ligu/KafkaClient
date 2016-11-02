using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Protocol;

namespace KafkaClient
{
    public static class ConsumerExtensions
    {
        public static Task<IImmutableList<Message>> FetchMessagesAsync(this IConsumer consumer, OffsetTopic offset, int maxCount, CancellationToken cancellationToken)
        {
            return consumer.FetchMessagesAsync(offset.TopicName, offset.PartitionId, offset.Offset, maxCount, cancellationToken);
        }
    }
}