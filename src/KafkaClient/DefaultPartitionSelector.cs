using System;
using System.Collections.Concurrent;
using System.Linq;
using KafkaClient.Common;
using KafkaClient.Protocol;

namespace KafkaClient
{
    public class DefaultPartitionSelector : IPartitionSelector
    {
        private readonly ConcurrentDictionary<string, int> _roundRobinTracker = new ConcurrentDictionary<string, int>();

        public MetadataPartition Select(MetadataTopic topic, byte[] key)
        {
            if (topic == null) throw new ArgumentNullException("topic");
            if (topic.Partitions.Count <= 0) throw new CachedMetadataException($"Topic: {topic.TopicName} has no partitions.") { Topic = topic.TopicName };

            //use round robin
            var partitions = topic.Partitions;
            if (key == null)
            {
                //use round robin
                var paritionIndex = _roundRobinTracker.AddOrUpdate(topic.TopicName, p => 0, (s, i) =>
                    {
                        return ((i + 1) % partitions.Count);
                    });

                return partitions[paritionIndex];
            }

            //use key hash
            var partitionId = Crc32Provider.Compute(key) % partitions.Count;
            var partition = partitions.FirstOrDefault(x => x.PartitionId == partitionId);

            if (partition == null)
                throw new CachedMetadataException($"Hash function return partition id: {partitionId}, but the available partitions are:{string.Join(",", partitions.Select(x => x.PartitionId))}") {
                    Topic = topic.TopicName,
                    Partition = (int)partitionId
                };

            return partition;
        }
    }
}