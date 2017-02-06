using System;
using System.Linq;
using KafkaClient.Common;
using KafkaClient.Protocol;

namespace KafkaClient
{
    public class PartitionSelector : IPartitionSelector
    {
        public MetadataResponse.Partition Select(MetadataResponse.Topic topic, ArraySegment<byte> key)
        {
            if (topic == null) throw new ArgumentNullException(nameof(topic));
            if (topic.partition_metadata.Count == 0) throw new RoutingException($"No partitions to choose on {topic}.");

            if (key.Count == 0) {
                return RoundRobinPartitionSelector.Singleton.Select(topic, key);
            }

            // use key hash
            var partitionId = Crc32.Compute(key) % topic.partition_metadata.Count;
            var partition = topic.partition_metadata.FirstOrDefault(x => x.partition_id == partitionId);
            if (partition != null) return partition;

            throw new RoutingException($"Hash function return partition {partitionId}, but the available partitions are {string.Join(",", topic.partition_metadata.Select(x => x.partition_id))}");
        }
    }
}