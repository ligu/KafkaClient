using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Protocol;
using KafkaClient.Tests.Helpers;
using NUnit.Framework;

namespace KafkaClient.Tests.Unit
{
    [TestFixture]
    [Category("Unit")]
    public class DefaultPartitionSelectorTests
    {
        private MetadataTopic _topicA;
        private MetadataTopic _topicB;

        [SetUp]
        public void Setup()
        {
            _topicA = new MetadataTopic("a", ErrorResponseCode.None, new [] {
                                            new MetadataPartition(0, 0),
                                            new MetadataPartition(1, 1),
                                        });
            _topicB = new MetadataTopic("b", ErrorResponseCode.None, new [] {
                                            new MetadataPartition(0, 0),
                                            new MetadataPartition(1, 1),
                                        });
        }

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public void RoundRobinShouldRollOver()
        {
            var selector = new PartitionSelector();

            var first = selector.Select(_topicA, null);
            var second = selector.Select(_topicA, null);
            var third = selector.Select(_topicA, null);

            Assert.That(first.PartitionId, Is.EqualTo(0));
            Assert.That(second.PartitionId, Is.EqualTo(1));
            Assert.That(third.PartitionId, Is.EqualTo(0));
        }

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public void RoundRobinShouldHandleMultiThreadedRollOver()
        {
            var selector = new PartitionSelector();
            var bag = new ConcurrentBag<MetadataPartition>();

            Parallel.For(0, 100, x => bag.Add(selector.Select(_topicA, null)));

            Assert.That(bag.Count(x => x.PartitionId == 0), Is.EqualTo(50));
            Assert.That(bag.Count(x => x.PartitionId == 1), Is.EqualTo(50));
        }

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public void RoundRobinShouldTrackEachTopicSeparately()
        {
            var selector = new PartitionSelector();

            var a1 = selector.Select(_topicA, null);
            var b1 = selector.Select(_topicB, null);
            var a2 = selector.Select(_topicA, null);
            var b2 = selector.Select(_topicB, null);

            Assert.That(a1.PartitionId, Is.EqualTo(0));
            Assert.That(a2.PartitionId, Is.EqualTo(1));

            Assert.That(b1.PartitionId, Is.EqualTo(0));
            Assert.That(b2.PartitionId, Is.EqualTo(1));
        }

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public void RoundRobinShouldEvenlyDistributeAcrossManyPartitions()
        {
            const int TotalPartitions = 100;
            var selector = new PartitionSelector();
            var partitions = new List<MetadataPartition>();
            for (int i = 0; i < TotalPartitions; i++)
            {
                partitions.Add(new MetadataPartition(i, i));
            }
            var topic = new MetadataTopic("a", partitions: partitions);

            var bag = new ConcurrentBag<MetadataPartition>();
            Parallel.For(0, TotalPartitions * 3, x => bag.Add(selector.Select(topic, null)));

            var eachPartitionHasThree = bag.GroupBy(x => x.PartitionId).Count();

            Assert.That(eachPartitionHasThree, Is.EqualTo(TotalPartitions), "Each partition should have received three selections.");
        }

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public void KeyHashShouldSelectEachPartitionType()
        {
            var selector = new PartitionSelector();

            var first = selector.Select(_topicA, CreateKeyForPartition(0));
            var second = selector.Select(_topicA, CreateKeyForPartition(1));

            Assert.That(first.PartitionId, Is.EqualTo(0));
            Assert.That(second.PartitionId, Is.EqualTo(1));
        }

        private byte[] CreateKeyForPartition(int partitionId)
        {
            while (true)
            {
                var key = Guid.NewGuid().ToString().ToIntSizedBytes();
                if ((Crc32Provider.Compute(key) % 2) == partitionId)
                    return key;
            }
        }

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        [ExpectedException(typeof(CachedMetadataException))]
        public void KeyHashShouldThrowExceptionWhenChoosesAPartitionIdThatDoesNotExist()
        {
            var selector = new PartitionSelector();
            var topic = new MetadataTopic("badPartition", partitions: new [] {
                                              new MetadataPartition(0, 0),
                                              new MetadataPartition(999, 1) 
                                          });

            selector.Select(topic, CreateKeyForPartition(1));
        }

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        [ExpectedException(typeof(CachedMetadataException))]
        public void SelectorShouldThrowExceptionWhenPartitionsAreEmpty()
        {
            var selector = new PartitionSelector();
            var topic = new MetadataTopic("emptyPartition");
            selector.Select(topic, CreateKeyForPartition(1));
        }
    }
}