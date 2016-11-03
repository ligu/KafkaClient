using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Connections;
using KafkaClient.Protocol;
using KafkaClient.Tests.Helpers;
using NUnit.Framework;
#pragma warning disable 1998

namespace KafkaClient.Tests
{
    [TestFixture]
    [Category("Integration")]
    public class ConsumerIntegrationTests
    {
        private readonly KafkaOptions _options;
        private readonly Uri _kafkaUri;
        private const int DefaultMaxMessageSetSize = 4096 * 8;
        private readonly int _partitionId = 0;
        private readonly IConnectionConfiguration _config;

        public ConsumerIntegrationTests()
        {
            _kafkaUri = IntegrationConfig.IntegrationUri;
            _config = new ConnectionConfiguration(TimeSpan.FromSeconds(10));
            _options = new KafkaOptions(IntegrationConfig.IntegrationUri, _config, log: new ConsoleLog());
        }

        [Test]
        public async Task CanFetch()
        {
            int partitionId = 0;
            var router = new BrokerRouter(new KafkaOptions(IntegrationConfig.IntegrationUri));

            var producer = new Producer(router);
            string messageValue = Guid.NewGuid().ToString();
            var response = await producer.SendMessageAsync(new Message(messageValue), IntegrationConfig.TopicName(), partitionId, CancellationToken.None);
            var offset = response.Offset;

            var fetch = new FetchRequest.Topic(IntegrationConfig.TopicName(), partitionId, offset, 32000);

            var fetchRequest = new FetchRequest(fetch, minBytes: 10);

            var r = await router.SendAsync(fetchRequest, IntegrationConfig.TopicName(), partitionId, CancellationToken.None);
            Assert.IsTrue(r.Topics.First().Messages.First().Value.ToUtf8String() == messageValue);
        }

        [Test]
        public async Task FetchMessagesSimpleTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_options);

            var topic = "ManualConsumerTestTopic";

            var producer = new Producer(brokerRouter);
            var consumer = new Consumer(brokerRouter, DefaultMaxMessageSetSize);

            var offset = await brokerRouter.GetTopicOffsetAsync(topic, _partitionId, CancellationToken.None);

            // Creating 5 messages
            var messages = CreateTestMessages(5, 1);

            await producer.SendMessagesAsync(messages, topic, _partitionId, new SendMessageConfiguration(ackTimeout: TimeSpan.FromSeconds(3)), CancellationToken.None);

            // Now let's consume
            var result = (await consumer.FetchMessagesAsync(offset, 5, CancellationToken.None)).ToList();

            CheckMessages(messages, result);
        }

        [Test]
        public async Task FetchMessagesCacheContainsAllRequestTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_options);

            var producer = new Producer(brokerRouter);
            var topic = IntegrationConfig.TopicName();
            var consumer = new Consumer(brokerRouter, DefaultMaxMessageSetSize);

            var offset = await brokerRouter.GetTopicOffsetAsync(topic, _partitionId, CancellationToken.None);

            // Creating 5 messages
            var messages = CreateTestMessages(10, 1);

            await producer.SendMessagesAsync(messages, topic, _partitionId, new SendMessageConfiguration(ackTimeout: TimeSpan.FromSeconds(3)), CancellationToken.None);

            // Now let's consume
            var result = (await consumer.FetchMessagesAsync(offset, 5, CancellationToken.None)).ToList();

            CheckMessages(messages.Take(5).ToList(), result);

            // Now let's consume again
            result = (await consumer.FetchMessagesAsync(offset.TopicName, offset.PartitionId, offset.Offset + 5, 5, CancellationToken.None)).ToList();

            CheckMessages(messages.Skip(5).ToList(), result);
        }

        [Test]
        public async Task FetchMessagesCacheContainsNoneOfRequestTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_options);

            var producer = new Producer(brokerRouter);
            var topic = IntegrationConfig.TopicName();
            var consumer = new Consumer(brokerRouter, DefaultMaxMessageSetSize);

            var offset = await brokerRouter.GetTopicOffsetAsync(topic, _partitionId, CancellationToken.None);

            // Creating 5 messages
            var messages = CreateTestMessages(10, 4096);

            await producer.SendMessagesAsync(messages, topic, _partitionId, new SendMessageConfiguration(ackTimeout: TimeSpan.FromSeconds(3)), CancellationToken.None);

            // Now let's consume
            var result = (await consumer.FetchMessagesAsync(offset, 7, CancellationToken.None)).ToList();

            CheckMessages(messages.Take(7).ToList(), result);

            // Now let's consume again
            result = (await consumer.FetchMessagesAsync(offset.TopicName, offset.PartitionId, offset.Offset + 5, 2, CancellationToken.None)).ToList();

            CheckMessages(messages.Skip(8).ToList(), result);
        }

        [Test]
        public async Task FetchMessagesCacheContainsPartOfRequestTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_options);

            var producer = new Producer(brokerRouter);
            var topic = IntegrationConfig.TopicName();
            var consumer = new Consumer(brokerRouter, DefaultMaxMessageSetSize);

            var offset = await brokerRouter.GetTopicOffsetAsync(topic, _partitionId, CancellationToken.None);

            // Creating 5 messages
            var messages = CreateTestMessages(10, 4096);

            await producer.SendMessagesAsync(messages, topic, _partitionId, new SendMessageConfiguration(ackTimeout: TimeSpan.FromSeconds(3)), CancellationToken.None);

            // Now let's consume
            var result = (await consumer.FetchMessagesAsync(offset, 5, CancellationToken.None)).ToList();

            CheckMessages(messages.Take(5).ToList(), result);

            // Now let's consume again
            result = (await consumer.FetchMessagesAsync(offset.TopicName, offset.PartitionId, offset.Offset + 5, 5, CancellationToken.None)).ToList();

            CheckMessages(messages.Skip(5).ToList(), result);
        }

        [Test]
        public async Task FetchMessagesNoNewMessagesInQueueTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_kafkaUri, new ConnectionFactory(), _config);

            var consumer = new Consumer(brokerRouter, DefaultMaxMessageSetSize);

            var offset = await brokerRouter.GetTopicOffsetAsync(IntegrationConfig.TopicName(), _partitionId, CancellationToken.None);

            // Now let's consume
            var result = (await consumer.FetchMessagesAsync(offset, 5, CancellationToken.None)).ToList();

            Assert.AreEqual(0, result.Count, "Should not get any messages");
        }

        [Test]
        public async Task FetchMessagesOffsetBiggerThanLastOffsetInQueueTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_kafkaUri, new ConnectionFactory(), _config, log: new ConsoleLog());

            var consumer = new Consumer(brokerRouter, DefaultMaxMessageSetSize);

            var offset = await brokerRouter.GetTopicOffsetAsync(IntegrationConfig.TopicName(), _partitionId, CancellationToken.None);

            try {
                // Now let's consume
                await consumer.FetchMessagesAsync(offset.TopicName, offset.PartitionId, offset.Offset + 1, 5, CancellationToken.None);
                Assert.Fail("should have thrown FetchOutOfRangeException");
            } catch (FetchOutOfRangeException ex) when (ex.Message.StartsWith("Kafka returned OffsetOutOfRange for Fetch request")) {
                Console.WriteLine(ex.ToString());
            }
        }

        [Test]
        public async Task FetchMessagesInvalidOffsetTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_kafkaUri, new ConnectionFactory(), _config);

            var consumer = new Consumer(brokerRouter, DefaultMaxMessageSetSize);

            var offset = -1;

            // Now let's consume
            Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await consumer.FetchMessagesAsync(IntegrationConfig.TopicName(), _partitionId, offset, 5, CancellationToken.None));
        }

        [Test]
        public async Task FetchMessagesTopicDoesntExist()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_kafkaUri, new ConnectionFactory(), _config);

            var topic = IntegrationConfig.TopicName();

            var consumer = new Consumer(brokerRouter, DefaultMaxMessageSetSize * 2);

            var offset = 0;

            // Now let's consume
            await consumer.FetchMessagesAsync(topic, _partitionId, offset, 5, CancellationToken.None);
        }

        [Test]
        public async Task FetchMessagesPartitionDoesntExist()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_kafkaUri, new ConnectionFactory(), _config);
            var partitionId = 100;
            var topic = IntegrationConfig.TopicName();

            var consumer = new Consumer(brokerRouter, DefaultMaxMessageSetSize * 2);

            var offset = 0;

            Assert.ThrowsAsync<CachedMetadataException>(async () => await consumer.FetchMessagesAsync(topic, partitionId, offset, 5, CancellationToken.None));
        }

        [Test]
        public async Task FetchMessagesBufferUnderRunTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_options);

            var smallMessageSet = 4096 / 2;

            var producer = new Producer(brokerRouter);
            var topic = IntegrationConfig.TopicName();
            var consumer = new Consumer(brokerRouter, smallMessageSet);

            var offset = await brokerRouter.GetTopicOffsetAsync(topic, _partitionId, CancellationToken.None);

            // Creating 5 messages
            var messages = CreateTestMessages(10, 4096);

            await producer.SendMessagesAsync(messages, topic, _partitionId, new SendMessageConfiguration(ackTimeout: TimeSpan.FromSeconds(3)), CancellationToken.None);

            try {
                // Now let's consume
                await consumer.FetchMessagesAsync(offset, 5, CancellationToken.None);
                Assert.Fail("should have thrown BufferUnderRunException");
            } catch (BufferUnderRunException ex) {
                Console.WriteLine(ex.ToString());
            }
        }

        [Test]
        public async Task FetchOffsetConsumerGroupDoesntExistTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_kafkaUri, new ConnectionFactory(), _config);
            var partitionId = 0;
            var consumerGroup = Guid.NewGuid().ToString();

            var topicName = IntegrationConfig.TopicName();
            await brokerRouter.GetTopicOffsetAsync(topicName, partitionId, consumerGroup, CancellationToken.None);
        }

        [Test]
        public async Task FetchOffsetPartitionDoesntExistTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_kafkaUri, new ConnectionFactory(), _config);
            var partitionId = 100;
            var consumerGroup = IntegrationConfig.ConsumerName();

            var topicName = IntegrationConfig.TopicName();
            Assert.ThrowsAsync<CachedMetadataException>(async () => await brokerRouter.GetTopicOffsetAsync(topicName, partitionId, consumerGroup, CancellationToken.None));
        }

        [Test]
        public async Task FetchOffsetTopicDoesntExistTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_kafkaUri, new ConnectionFactory(), _config);

            var topic = IntegrationConfig.TopicName();
            var consumerGroup = IntegrationConfig.ConsumerName();

            await brokerRouter.GetTopicOffsetAsync(topic, _partitionId, consumerGroup, CancellationToken.None);
        }

        [Test]
        public async Task FetchOffsetConsumerGroupExistsTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_kafkaUri, new ConnectionFactory(), _config);
            var partitionId = 0;
            var consumerGroup = IntegrationConfig.ConsumerName();

            var offset = 5L;

            var topicName = IntegrationConfig.TopicName();
            await brokerRouter.CommitTopicOffsetAsync(topicName, partitionId, consumerGroup, offset, CancellationToken.None);
            var res = await brokerRouter.GetTopicOffsetAsync(topicName, _partitionId, consumerGroup, CancellationToken.None);

            Assert.AreEqual(offset, res.Offset);
        }

        [Test]
        public async Task FetchOffsetConsumerGroupArgumentNull([Values(null, "")] string group)
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_kafkaUri, new ConnectionFactory(), _config);
            var partitionId = 0;
            var consumerGroup = IntegrationConfig.ConsumerName();

            var topicName = IntegrationConfig.TopicName();

            var offset = 5;

            await brokerRouter.CommitTopicOffsetAsync(topicName, partitionId, consumerGroup, offset, CancellationToken.None);
            Assert.ThrowsAsync<ArgumentNullException>(async () => await brokerRouter.GetTopicOffsetAsync(topicName, partitionId, group, CancellationToken.None));
        }

        [Test]
        public async Task UpdateOrCreateOffsetConsumerGroupExistsTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_kafkaUri, new ConnectionFactory(), _config);
            var partitionId = 0;
            var consumerGroup = IntegrationConfig.ConsumerName();

            var topicName = IntegrationConfig.TopicName();
            var offest = 5;
            var newOffset = 10;

            await brokerRouter.GetTopicOffsetAsync(topicName, partitionId, CancellationToken.None);
            await brokerRouter.CommitTopicOffsetAsync(topicName, partitionId, consumerGroup, offest, CancellationToken.None);
            var res = await brokerRouter.GetTopicOffsetAsync(topicName, partitionId, consumerGroup, CancellationToken.None);
            Assert.AreEqual(offest, res.Offset);

            await brokerRouter.CommitTopicOffsetAsync(topicName, partitionId, consumerGroup, newOffset, CancellationToken.None);
            res = await brokerRouter.GetTopicOffsetAsync(topicName, partitionId, consumerGroup, CancellationToken.None);

            Assert.AreEqual(newOffset, res.Offset);
        }

        [Test]
        public async Task UpdateOrCreateOffsetPartitionDoesntExistTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_kafkaUri, new ConnectionFactory(), _config);
            var partitionId = 100;
            var consumerGroup = Guid.NewGuid().ToString();

            var topicName = IntegrationConfig.TopicName();

            var offest = 5;

            Assert.ThrowsAsync<CachedMetadataException>(async () => await brokerRouter.CommitTopicOffsetAsync(topicName, partitionId, consumerGroup, offest, CancellationToken.None));
        }

        [Test]
        public async Task UpdateOrCreateOffsetTopicDoesntExistTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_kafkaUri, new ConnectionFactory(), _config);
            var partitionId = 0;
            var topic = IntegrationConfig.TopicName();
            var consumerGroup = IntegrationConfig.ConsumerName();

            var offest = 5;

            await brokerRouter.CommitTopicOffsetAsync(topic, partitionId, consumerGroup, offest, CancellationToken.None);
        }

        [Test]
        public async Task UpdateOrCreateOffsetConsumerGroupArgumentNull([Values(null, "")] string group)
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_kafkaUri, new ConnectionFactory(), _config);
            var partitionId = 0;
            var topic = IntegrationConfig.TopicName();

            var offest = 5;

            Assert.ThrowsAsync<ArgumentNullException>(async () => await brokerRouter.CommitTopicOffsetAsync(topic, partitionId, group, offest, CancellationToken.None));
        }

        [Test]
        public async Task UpdateOrCreateOffsetNegativeOffsetTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_kafkaUri, new ConnectionFactory(), _config);
            var partitionId = 0;
            var topic = IntegrationConfig.TopicName();
            var consumerGroup = IntegrationConfig.ConsumerName();

            var offest = -5;

            Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await brokerRouter.CommitTopicOffsetAsync(topic, partitionId, consumerGroup, offest, CancellationToken.None));
        }

        [Test]
        public async Task FetchLastOffsetSimpleTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_kafkaUri, new ConnectionFactory(), _config);

            var topic = IntegrationConfig.TopicName();

            var offset = await brokerRouter.GetTopicOffsetAsync(topic, _partitionId, CancellationToken.None);

            Assert.AreNotEqual(-1, offset.Offset);
        }

        [Test]
        public async Task FetchLastOffsetPartitionDoesntExistTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_kafkaUri, new ConnectionFactory(), _config);
            var partitionId = 100;
            var topic = IntegrationConfig.TopicName();

            Assert.ThrowsAsync<CachedMetadataException>(async () => await brokerRouter.GetTopicOffsetAsync(topic, partitionId, CancellationToken.None));
        }

        [Test]
        public async Task FetchLastOffsetTopicDoesntExistTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_kafkaUri, new ConnectionFactory(), _config, log: new ConsoleLog());

            var topic = IntegrationConfig.TopicName();

            await brokerRouter.GetTopicOffsetAsync(topic, _partitionId, CancellationToken.None);
        }

        private void CheckMessages(List<Message> expected, List<Message> actual)
        {
            Assert.AreEqual(expected.Count(), actual.Count(), "Didn't get all messages");

            foreach (var message in expected)
            {
                Assert.IsTrue(actual.Any(m => m.Value.SequenceEqual(message.Value)), "Didn't get the same messages");
            }
        }

        private List<Message> CreateTestMessages(int amount, int messageSize)
        {
            var messages = new List<Message>();

            for (var i = 1; i <= amount; i++)
            {
                var payload = new List<byte>(messageSize);

                for (var j = 0; j < messageSize; j++)
                {
                    payload.Add(Convert.ToByte(1));
                }

                messages.Add(new Message(payload.ToArray(), 0));
            }

            return messages;
        }
    }
}