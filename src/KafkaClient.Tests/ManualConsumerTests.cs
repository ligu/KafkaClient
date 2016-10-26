using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Connections;
using KafkaClient.Protocol;
using KafkaClient.Tests.Helpers;
using NUnit.Framework;
using NUnit.Framework.Constraints;
#pragma warning disable 1998

namespace KafkaClient.Tests
{
    [TestFixture]
    [Category("Integration")]
    public class ManualConsumerTests
    {
        private readonly KafkaOptions _options;
        private readonly Uri _kafkaUri;
        private const int DefaultMaxMessageSetSize = 4096 * 8;
        private readonly int _partitionId = 0;
        private readonly IConnectionConfiguration _config;

        public ManualConsumerTests()
        {
            _kafkaUri = IntegrationConfig.IntegrationUri;
            _options = new KafkaOptions(IntegrationConfig.IntegrationUri);
            _config = new ConnectionConfiguration(TimeSpan.FromMinutes(1));
        }

        [Test]
        public async Task FetchMessagesSimpleTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_options);

            var topic = "ManualConsumerTestTopic";

            Producer producer = new Producer(brokerRouter);
            ManualConsumer consumer = new ManualConsumer(_partitionId, topic, brokerRouter, "TestClient", DefaultMaxMessageSetSize);

            var offset = await consumer.FetchLastOffsetAsync(CancellationToken.None);

            // Creating 5 messages
            List<Message> messages = CreateTestMessages(5, 1);

            await producer.SendMessagesAsync(messages, topic, _partitionId, new SendMessageConfiguration(ackTimeout: TimeSpan.FromSeconds(3)), CancellationToken.None);

            // Now let's consume
            var result = (await consumer.FetchMessagesAsync(5, offset, CancellationToken.None)).ToList();

            CheckMessages(messages, result);
        }

        [Test]
        public async Task FetchMessagesCacheContainsAllRequestTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_options);

            Producer producer = new Producer(brokerRouter);
            var topic = IntegrationConfig.TopicName();
            ManualConsumer consumer = new ManualConsumer(_partitionId, topic, brokerRouter, "TestClient", DefaultMaxMessageSetSize);

            var offset = await consumer.FetchLastOffsetAsync(CancellationToken.None);

            // Creating 5 messages
            List<Message> messages = CreateTestMessages(10, 1);

            await producer.SendMessagesAsync(messages, topic, _partitionId, new SendMessageConfiguration(ackTimeout: TimeSpan.FromSeconds(3)), CancellationToken.None);

            // Now let's consume
            var result = (await consumer.FetchMessagesAsync(5, offset, CancellationToken.None)).ToList();

            CheckMessages(messages.Take(5).ToList(), result);

            // Now let's consume again
            result = (await consumer.FetchMessagesAsync(5, offset + 5, CancellationToken.None)).ToList();

            CheckMessages(messages.Skip(5).ToList(), result);
        }

        [Test]
        public async Task FetchMessagesCacheContainsNoneOfRequestTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_options);

            Producer producer = new Producer(brokerRouter);
            var topic = IntegrationConfig.TopicName();
            ManualConsumer consumer = new ManualConsumer(_partitionId, topic, brokerRouter, "TestClient", DefaultMaxMessageSetSize);

            var offset = await consumer.FetchLastOffsetAsync(CancellationToken.None);

            // Creating 5 messages
            List<Message> messages = CreateTestMessages(10, 4096);

            await producer.SendMessagesAsync(messages, topic, _partitionId, new SendMessageConfiguration(ackTimeout: TimeSpan.FromSeconds(3)), CancellationToken.None);

            // Now let's consume
            var result = (await consumer.FetchMessagesAsync(7, offset, CancellationToken.None)).ToList();

            CheckMessages(messages.Take(7).ToList(), result);

            // Now let's consume again
            result = (await consumer.FetchMessagesAsync(2, offset + 8, CancellationToken.None)).ToList();

            CheckMessages(messages.Skip(8).ToList(), result);
        }

        [Test]
        public async Task FetchMessagesCacheContainsPartOfRequestTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_options);

            Producer producer = new Producer(brokerRouter);
            var topic = IntegrationConfig.TopicName();
            ManualConsumer consumer = new ManualConsumer(_partitionId, topic, brokerRouter, "TestClient", DefaultMaxMessageSetSize);

            var offset = await consumer.FetchLastOffsetAsync(CancellationToken.None);

            // Creating 5 messages
            List<Message> messages = CreateTestMessages(10, 4096);

            await producer.SendMessagesAsync(messages, topic, _partitionId, new SendMessageConfiguration(ackTimeout: TimeSpan.FromSeconds(3)), CancellationToken.None);

            // Now let's consume
            var result = (await consumer.FetchMessagesAsync(5, offset, CancellationToken.None)).ToList();

            CheckMessages(messages.Take(5).ToList(), result);

            // Now let's consume again
            result = (await consumer.FetchMessagesAsync(5, offset + 5, CancellationToken.None)).ToList();

            CheckMessages(messages.Skip(5).ToList(), result);
        }

        [Test]
        public async Task FetchMessagesNoNewMessagesInQueueTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_kafkaUri, new ConnectionFactory(), _config);

            ManualConsumer consumer = new ManualConsumer(_partitionId, IntegrationConfig.TopicName(), brokerRouter, "TestClient", DefaultMaxMessageSetSize);

            var offset = await consumer.FetchLastOffsetAsync(CancellationToken.None);

            // Now let's consume
            var result = (await consumer.FetchMessagesAsync(5, offset, CancellationToken.None)).ToList();

            Assert.AreEqual(0, result.Count, "Should not get any messages");
        }

        [Test]
        public async Task FetchMessagesOffsetBiggerThanLastOffsetInQueueTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_kafkaUri, new ConnectionFactory(), _config);

            ManualConsumer consumer = new ManualConsumer(_partitionId, IntegrationConfig.TopicName(), brokerRouter, "TestClient", DefaultMaxMessageSetSize);

            var offset = await consumer.FetchLastOffsetAsync(CancellationToken.None);

            Assert.ThrowsAsync(Is.InstanceOf<FetchOutOfRangeException>().With.Message.StartsWith("Kafka returned OffsetOutOfRange for Fetch request"), 
                async () => await consumer.FetchMessagesAsync(5, offset + 1, CancellationToken.None));
        }

        [Test]
        public async Task FetchMessagesInvalidOffsetTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_kafkaUri, new ConnectionFactory(), _config);

            ManualConsumer consumer = new ManualConsumer(_partitionId, IntegrationConfig.TopicName(), brokerRouter, "TestClient", DefaultMaxMessageSetSize);

            var offset = -1;

            // Now let's consume
            Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await consumer.FetchMessagesAsync(5, offset, CancellationToken.None));
        }

        [Test]
        public async Task FetchMessagesTopicDoesntExist()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_kafkaUri, new ConnectionFactory(), _config);

            var topic = IntegrationConfig.TopicName();

            ManualConsumer consumer = new ManualConsumer(_partitionId, topic, brokerRouter, "TestClient", DefaultMaxMessageSetSize * 2);

            var offset = 0;

            // Now let's consume
            Assert.ThrowsAsync<RequestException>(async () => await consumer.FetchMessagesAsync(5, offset, CancellationToken.None));
        }

        [Test]
        public async Task FetchMessagesPartitionDoesntExist()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_kafkaUri, new ConnectionFactory(), _config);
            var partitionId = 100;
            var topic = IntegrationConfig.TopicName();

            ManualConsumer consumer = new ManualConsumer(partitionId, topic, brokerRouter, "TestClient", DefaultMaxMessageSetSize * 2);

            var offset = 0;

            Assert.ThrowsAsync<CachedMetadataException>(async () => await consumer.FetchMessagesAsync(5, offset, CancellationToken.None));
        }

        [Test]
        public async Task FetchMessagesBufferUnderRunTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_options);

            var smallMessageSet = 4096 / 2;

            Producer producer = new Producer(brokerRouter);
            var topic = IntegrationConfig.TopicName();
            ManualConsumer consumer = new ManualConsumer(_partitionId, topic, brokerRouter, "TestClient", smallMessageSet);

            var offset = await consumer.FetchLastOffsetAsync(CancellationToken.None);

            // Creating 5 messages
            List<Message> messages = CreateTestMessages(10, 4096);

            await producer.SendMessagesAsync(messages, topic, _partitionId, new SendMessageConfiguration(ackTimeout: TimeSpan.FromSeconds(3)), CancellationToken.None);

            // Now let's consume
            Assert.ThrowsAsync<BufferUnderRunException>(async () => await consumer.FetchMessagesAsync(5, offset, CancellationToken.None));
        }

        [Test]
        public async Task FetchOffsetConsumerGroupDoesntExistTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_kafkaUri, new ConnectionFactory(), _config);
            var partitionId = 0;
            var consumerGroup = Guid.NewGuid().ToString();

            ManualConsumer consumer = new ManualConsumer(partitionId, IntegrationConfig.TopicName(), brokerRouter, "TestClient", DefaultMaxMessageSetSize);

            Assert.ThrowsAsync<RequestException>(async () => await consumer.FetchOffsetAsync(consumerGroup, CancellationToken.None));
        }

        [Test]
        public async Task FetchOffsetPartitionDoesntExistTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_kafkaUri, new ConnectionFactory(), _config);
            var partitionId = 100;
            var consumerGroup = IntegrationConfig.ConsumerName();

            ManualConsumer consumer = new ManualConsumer(partitionId, IntegrationConfig.TopicName(), brokerRouter, "TestClient", DefaultMaxMessageSetSize);

            Assert.ThrowsAsync<CachedMetadataException>(async () => await consumer.FetchOffsetAsync(consumerGroup, CancellationToken.None));
        }

        [Test]
        public async Task FetchOffsetTopicDoesntExistTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_kafkaUri, new ConnectionFactory(), _config);

            var topic = IntegrationConfig.TopicName();
            var consumerGroup = IntegrationConfig.ConsumerName();

            ManualConsumer consumer = new ManualConsumer(_partitionId, topic, brokerRouter, "TestClient", DefaultMaxMessageSetSize);

            Assert.ThrowsAsync<RequestException>(async () => await consumer.FetchOffsetAsync(consumerGroup, CancellationToken.None));
        }

        [Test]
        public async Task FetchOffsetConsumerGroupExistsTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_kafkaUri, new ConnectionFactory(), _config);
            var partitionId = 0;
            var consumerGroup = IntegrationConfig.ConsumerName();

            ManualConsumer consumer = new ManualConsumer(partitionId, IntegrationConfig.TopicName(), brokerRouter, "TestClient", DefaultMaxMessageSetSize);

            var offest = 5;

            await consumer.UpdateOrCreateOffsetAsync(consumerGroup, offest, CancellationToken.None);
            var res = await consumer.FetchOffsetAsync(consumerGroup, CancellationToken.None);

            Assert.AreEqual(offest, res);
        }

        [Test]
        public async Task FetchOffsetConsumerGroupArgumentNull([Values(null, "")] string group)
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_kafkaUri, new ConnectionFactory(), _config);
            var partitionId = 0;
            var consumerGroup = IntegrationConfig.ConsumerName();

            ManualConsumer consumer = new ManualConsumer(partitionId, IntegrationConfig.TopicName(), brokerRouter, "TestClient", DefaultMaxMessageSetSize);

            var offest = 5;

            await consumer.UpdateOrCreateOffsetAsync(consumerGroup, offest, CancellationToken.None);
            Assert.ThrowsAsync<ArgumentNullException>(async () => await consumer.FetchOffsetAsync(group, CancellationToken.None));
        }

        [Test]
        public async Task UpdateOrCreateOffsetConsumerGroupDoesntExistTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_kafkaUri, new ConnectionFactory(), _config);
            var partitionId = 0;
            var consumerGroup = Guid.NewGuid().ToString();

            ManualConsumer consumer = new ManualConsumer(partitionId, IntegrationConfig.TopicName(), brokerRouter, "TestClient", DefaultMaxMessageSetSize);

            var offest = 5;

            await consumer.UpdateOrCreateOffsetAsync(consumerGroup, offest, CancellationToken.None);
            var res = await consumer.FetchOffsetAsync(consumerGroup, CancellationToken.None);

            Assert.AreEqual(offest, res);
        }

        [Test]
        public async Task UpdateOrCreateOffsetConsumerGroupExistsTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_kafkaUri, new ConnectionFactory(), _config);
            var partitionId = 0;
            var consumerGroup = IntegrationConfig.ConsumerName();

            ManualConsumer consumer = new ManualConsumer(partitionId, IntegrationConfig.TopicName(), brokerRouter, "TestClient", DefaultMaxMessageSetSize);

            var offest = 5;
            var newOffset = 10;

            await consumer.UpdateOrCreateOffsetAsync(consumerGroup, offest, CancellationToken.None);
            var res = await consumer.FetchOffsetAsync(consumerGroup, CancellationToken.None);
            Assert.AreEqual(offest, res);

            await consumer.UpdateOrCreateOffsetAsync(consumerGroup, newOffset, CancellationToken.None);

            res = await consumer.FetchOffsetAsync(consumerGroup, CancellationToken.None);

            Assert.AreEqual(newOffset, res);
        }

        [Test]
        public async Task UpdateOrCreateOffsetPartitionDoesntExistTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_kafkaUri, new ConnectionFactory(), _config);
            var partitionId = 100;
            var consumerGroup = Guid.NewGuid().ToString();

            ManualConsumer consumer = new ManualConsumer(partitionId, IntegrationConfig.TopicName(), brokerRouter, "TestClient", DefaultMaxMessageSetSize);

            var offest = 5;

            Assert.ThrowsAsync<CachedMetadataException>(async () => await consumer.UpdateOrCreateOffsetAsync(consumerGroup, offest, CancellationToken.None));
        }

        [Test]
        [Ignore("This test is currently faulty, can't have UpdateOrCreateOffset behave differently than FetchOffset")]
        public async Task UpdateOrCreateOffsetTopicDoesntExistTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_kafkaUri, new ConnectionFactory(), _config);
            var partitionId = 0;
            var topic = IntegrationConfig.TopicName();
            var consumerGroup = IntegrationConfig.ConsumerName();

            ManualConsumer consumer = new ManualConsumer(partitionId, topic, brokerRouter, "TestClient", DefaultMaxMessageSetSize);

            var offest = 5;

            await consumer.UpdateOrCreateOffsetAsync(consumerGroup, offest, CancellationToken.None);
        }

        [Test]
        public async Task UpdateOrCreateOffsetConsumerGroupArgumentNull([Values(null, "")] string group)
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_kafkaUri, new ConnectionFactory(), _config);
            var partitionId = 0;
            var topic = IntegrationConfig.TopicName();

            ManualConsumer consumer = new ManualConsumer(partitionId, topic, brokerRouter, "TestClient", DefaultMaxMessageSetSize);

            var offest = 5;

            Assert.ThrowsAsync<ArgumentNullException>(async () => await consumer.UpdateOrCreateOffsetAsync(group, offest, CancellationToken.None));
        }

        [Test]
        public async Task UpdateOrCreateOffsetNegativeOffsetTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_kafkaUri, new ConnectionFactory(), _config);
            var partitionId = 0;
            var topic = IntegrationConfig.TopicName();
            var consumerGroup = IntegrationConfig.ConsumerName();

            ManualConsumer consumer = new ManualConsumer(partitionId, topic, brokerRouter, "TestClient", DefaultMaxMessageSetSize);

            var offest = -5;

            Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await consumer.UpdateOrCreateOffsetAsync(consumerGroup, offest, CancellationToken.None));
        }

        [Test]
        public async Task FetchLastOffsetSimpleTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_kafkaUri, new ConnectionFactory(), _config);

            var topic = IntegrationConfig.TopicName();

            ManualConsumer consumer = new ManualConsumer(_partitionId, topic, brokerRouter, "TestClient", DefaultMaxMessageSetSize * 2);

            var offset = await consumer.FetchLastOffsetAsync(CancellationToken.None);

            Assert.AreNotEqual(-1, offset);
        }

        [Test]
        public async Task FetchLastOffsetPartitionDoesntExistTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_kafkaUri, new ConnectionFactory(), _config);
            var partitionId = 100;
            var topic = IntegrationConfig.TopicName();

            ManualConsumer consumer = new ManualConsumer(partitionId, topic, brokerRouter, "TestClient", DefaultMaxMessageSetSize * 2);

            Assert.ThrowsAsync<CachedMetadataException>(async () => await consumer.FetchLastOffsetAsync(CancellationToken.None));
        }

        [Test]
        public async Task FetchLastOffsetTopicDoesntExistTest()
        {
            // Creating a broker router and a protocol gateway for the producer and consumer
            var brokerRouter = new BrokerRouter(_kafkaUri, new ConnectionFactory(), _config);

            var topic = IntegrationConfig.TopicName();

            ManualConsumer consumer = new ManualConsumer(_partitionId, topic, brokerRouter, "TestClient", DefaultMaxMessageSetSize * 2);

            Assert.ThrowsAsync<RequestException>(async () => await consumer.FetchLastOffsetAsync(CancellationToken.None));
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
            List<Message> messages = new List<Message>();

            for (int i = 1; i <= amount; i++)
            {
                List<byte> payload = new List<byte>(messageSize);

                for (int j = 0; j < messageSize; j++)
                {
                    payload.Add(Convert.ToByte(1));
                }

                messages.Add(new Message(payload.ToArray(), 0));
            }

            return messages;
        }
    }
}