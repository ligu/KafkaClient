using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Connections;
using KafkaClient.Protocol;
using KafkaClient.Protocol.Types;
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
        private readonly IConsumerConfiguration _consumerConfig;

        public ConsumerIntegrationTests()
        {
            _kafkaUri = TestConfig.IntegrationUri;
            _config = new ConnectionConfiguration(ConnectionConfiguration.Defaults.ConnectionRetry(TimeSpan.FromSeconds(10)), requestTimeout: TimeSpan.FromSeconds(1));
            _consumerConfig = new ConsumerConfiguration(maxPartitionFetchBytes: DefaultMaxMessageSetSize);
            _options = new KafkaOptions(TestConfig.IntegrationUri, new ConnectionConfiguration(ConnectionConfiguration.Defaults.ConnectionRetry(TimeSpan.FromSeconds(10)), requestTimeout: TimeSpan.FromSeconds(10)), log: TestConfig.Log, consumerConfiguration: _consumerConfig);
        }

        [Test]
        public async Task CanFetch()
        {
            const int partitionId = 0;
            using (var router = new Router(new KafkaOptions(TestConfig.IntegrationUri))) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        string messageValue = Guid.NewGuid().ToString();
                        var response = await producer.SendMessageAsync(new Message(messageValue), TestConfig.TopicName(), partitionId, CancellationToken.None);
                        var offset = response.Offset;

                        var fetch = new FetchRequest.Topic(TestConfig.TopicName(), partitionId, offset, 32000);

                        var fetchRequest = new FetchRequest(fetch, minBytes: 10);

                        var r = await router.SendAsync(fetchRequest, TestConfig.TopicName(), partitionId, CancellationToken.None);
                        Assert.IsTrue(r.Topics.First().Messages.First().Value.ToUtf8String() == messageValue);
                    }
                });
            }
        }

        [Test]
        public async Task FetchMessagesSimpleTest()
        {
            using (var router = new Router(_options)) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        var consumer = new Consumer(router, _consumerConfig);

                        var offset = await router.GetTopicOffsetAsync(topicName, _partitionId, CancellationToken.None);

                        // Creating 5 messages
                        var messages = CreateTestMessages(5, 1);

                        await producer.SendMessagesAsync(messages, topicName, _partitionId, new SendMessageConfiguration(ackTimeout: TimeSpan.FromSeconds(3)), CancellationToken.None);

                        // Now let's consume
                        var result = (await consumer.FetchMessagesAsync(offset, 5, CancellationToken.None)).ToList();

                        CheckMessages(messages, result);
                    }
                });
            }
        }

        [Test]
        public async Task FetchMessagesCacheContainsAllRequestTest()
        {
            using (var router = new Router(_options)) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        var consumer = new Consumer(router, _consumerConfig);

                        var offset = await router.GetTopicOffsetAsync(topicName, _partitionId, CancellationToken.None);

                        // Creating 5 messages
                        var messages = CreateTestMessages(10, 1);

                        await producer.SendMessagesAsync(messages, topicName, _partitionId, new SendMessageConfiguration(ackTimeout: TimeSpan.FromSeconds(3)), CancellationToken.None);

                        // Now let's consume
                        var result = (await consumer.FetchMessagesAsync(offset, 5, CancellationToken.None)).ToList();

                        CheckMessages(messages.Take(5).ToList(), result);

                        // Now let's consume again
                        result = (await consumer.FetchMessagesAsync(offset.TopicName, offset.PartitionId, offset.Offset + 5, 5, CancellationToken.None)).ToList();

                        CheckMessages(messages.Skip(5).ToList(), result);
                    }
                });
            }
        }

        [Test]
        public async Task FetchMessagesCacheContainsNoneOfRequestTest()
        {
            using (var router = new Router(_options)) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        var consumer = new Consumer(router, _consumerConfig);

                        var offset = await router.GetTopicOffsetAsync(topicName, _partitionId, CancellationToken.None);

                        // Creating 5 messages
                        var messages = CreateTestMessages(10, 4096);

                        await producer.SendMessagesAsync(messages, topicName, _partitionId, new SendMessageConfiguration(ackTimeout: TimeSpan.FromSeconds(3)), CancellationToken.None);

                        // Now let's consume
                        var result = (await consumer.FetchMessagesAsync(offset, 7, CancellationToken.None)).ToList();

                        CheckMessages(messages.Take(7).ToList(), result);

                        // Now let's consume again
                        result = (await consumer.FetchMessagesAsync(offset.TopicName, offset.PartitionId, offset.Offset + 5, 2, CancellationToken.None)).ToList();

                        CheckMessages(messages.Skip(8).ToList(), result);
                    }
                });
            }
        }

        [Test]
        public async Task FetchMessagesCacheContainsPartOfRequestTest()
        {
            using (var router = new Router(_options)) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        var consumer = new Consumer(router, _consumerConfig);

                        var offset = await router.GetTopicOffsetAsync(topicName, _partitionId, CancellationToken.None);

                        // Creating 5 messages
                        var messages = CreateTestMessages(10, 4096);

                        await producer.SendMessagesAsync(messages, topicName, _partitionId, new SendMessageConfiguration(ackTimeout: TimeSpan.FromSeconds(3)), CancellationToken.None);

                        // Now let's consume
                        var result = (await consumer.FetchMessagesAsync(offset, 5, CancellationToken.None)).ToList();

                        CheckMessages(messages.Take(5).ToList(), result);

                        // Now let's consume again
                        result = (await consumer.FetchMessagesAsync(offset.TopicName, offset.PartitionId, offset.Offset + 5, 5, CancellationToken.None)).ToList();

                        CheckMessages(messages.Skip(5).ToList(), result);
                    }
                });
            }
        }

        [Test]
        public async Task FetchMessagesNoNewMessagesInQueueTest()
        {
            using (var router = new Router(_options)) {
                await router.TemporaryTopicAsync(async topicName => {
                    var consumer = new Consumer(router, _consumerConfig);

                    var offset = await router.GetTopicOffsetAsync(TestConfig.TopicName(), _partitionId, CancellationToken.None);

                    // Now let's consume
                    var result = (await consumer.FetchMessagesAsync(offset, 5, CancellationToken.None)).ToList();

                    Assert.AreEqual(0, result.Count, "Should not get any messages");
                });
            }
        }

        [Test]
        public async Task FetchMessagesOffsetBiggerThanLastOffsetInQueueTest()
        {
            using (var router = new Router(_kafkaUri, new ConnectionFactory(), _config, log: TestConfig.Log)) {
                await router.TemporaryTopicAsync(async topicName => {
                    var consumer = new Consumer(router, _consumerConfig);

                    var offset = await router.GetTopicOffsetAsync(TestConfig.TopicName(), _partitionId, CancellationToken.None);

                    try {
                        // Now let's consume
                        await consumer.FetchMessagesAsync(offset.TopicName, offset.PartitionId, offset.Offset + 1, 5, CancellationToken.None);
                        Assert.Fail("should have thrown FetchOutOfRangeException");
                    } catch (FetchOutOfRangeException ex) when (ex.Message.StartsWith("Kafka returned OffsetOutOfRange for Fetch request")) {
                        Console.WriteLine(ex.ToString());
                    }
                });
            }
        }

        [Test]
        public async Task FetchMessagesInvalidOffsetTest()
        {
            using (var router = new Router(_kafkaUri, new ConnectionFactory(), _config)) {
                await router.TemporaryTopicAsync(async topicName => {
                    var consumer = new Consumer(router, _consumerConfig);
                
                    var offset = -1;

                    // Now let's consume
                    Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await consumer.FetchMessagesAsync(topicName, _partitionId, offset, 5, CancellationToken.None));
                });
            }
        }

        [Test]
        public async Task FetchMessagesTopicDoesntExist()
        {
            using (var router = new Router(_kafkaUri, new ConnectionFactory(), _config)) {
                var topicName = TestConfig.TopicName();
                try {
                    await router.SendToAnyAsync(new DeleteTopicsRequest(new [] { topicName }, TimeSpan.FromSeconds(1)), CancellationToken.None);
                } catch (RequestException ex) when (ex.ErrorCode == ErrorResponseCode.TopicAlreadyExists) {
                    // ignore
                }
                var consumer = new Consumer(router, new ConsumerConfiguration(maxPartitionFetchBytes: DefaultMaxMessageSetSize * 2));

                var offset = 0;

                // Now let's consume
                try {
                    await consumer.FetchMessagesAsync(topicName, _partitionId, offset, 5, CancellationToken.None);
                    Assert.Fail("should have thrown CachedMetadataException");
                } catch (CachedMetadataException ex) when (ex.Message == "Unable to refresh metadata") {
                    // expected
                }
            }
        }

        [Test]
        public async Task FetchMessagesPartitionDoesntExist()
        {
            using (var router = new Router(_kafkaUri, new ConnectionFactory(), _config)) {
                await router.TemporaryTopicAsync(async topicName => {
                    var partitionId = 100;

                    var consumer = new Consumer(router, new ConsumerConfiguration(maxPartitionFetchBytes: DefaultMaxMessageSetSize * 2));

                    var offset = 0;

                    Assert.ThrowsAsync<CachedMetadataException>(async () => await consumer.FetchMessagesAsync(topicName, partitionId, offset, 5, CancellationToken.None));
                });
            }
        }

        [Test]
        public async Task FetchMessagesBufferUnderRunTest()
        {
            using (var router = new Router(_options)) {
                await router.TemporaryTopicAsync(async topicName => {
                    var smallMessageSet = 4096 / 2;

                    var producer = new Producer(router);
                    var consumer = new Consumer(router, new ConsumerConfiguration(maxPartitionFetchBytes: smallMessageSet));

                    var offset = await router.GetTopicOffsetAsync(topicName, _partitionId, CancellationToken.None);

                    // Creating 5 messages
                    var messages = CreateTestMessages(10, 4096);

                    await producer.SendMessagesAsync(messages, topicName, _partitionId, new SendMessageConfiguration(ackTimeout: TimeSpan.FromSeconds(3)), CancellationToken.None);

                    try {
                        // Now let's consume
                        await consumer.FetchMessagesAsync(offset, 5, CancellationToken.None);
                        Assert.Fail("should have thrown BufferUnderRunException");
                    } catch (BufferUnderRunException ex) {
                        Console.WriteLine(ex.ToString());
                    }
                });
            }
        }

        [Test]
        public async Task FetchOffsetConsumerGroupDoesntExistTest()
        {
            using (var router = new Router(_kafkaUri, new ConnectionFactory(), _config)) {
                await router.TemporaryTopicAsync(async topicName => {
                    var partitionId = 0;
                    var consumerGroup = Guid.NewGuid().ToString();

                    await router.GetTopicOffsetAsync(topicName, partitionId, consumerGroup, CancellationToken.None);
                });
            }
        }

        [Test]
        public async Task FetchOffsetPartitionDoesntExistTest()
        {
            using (var router = new Router(_kafkaUri, new ConnectionFactory(), _config)) {
                await router.TemporaryTopicAsync(async topicName => {
                    var partitionId = 100;
                    var consumerGroup = TestConfig.GroupId();

                    Assert.ThrowsAsync<CachedMetadataException>(async () => await router.GetTopicOffsetAsync(topicName, partitionId, consumerGroup, CancellationToken.None));
                });
            }
        }

        [Test]
        public async Task FetchOffsetTopicDoesntExistTest()
        {
            using (var router = new Router(_kafkaUri, new ConnectionFactory(), _config)) {
                var topicName = TestConfig.TopicName();
                try {
                    await router.SendToAnyAsync(new DeleteTopicsRequest(new [] { topicName }, TimeSpan.FromSeconds(1)), CancellationToken.None);
                } catch (RequestException ex) when (ex.ErrorCode == ErrorResponseCode.TopicAlreadyExists) {
                    // ignore
                }

                var consumerGroup = TestConfig.GroupId();
                try {
                    await router.GetTopicOffsetAsync(topicName, _partitionId, consumerGroup, CancellationToken.None);
                    Assert.Fail("should have thrown CachedMetadataException");
                } catch (CachedMetadataException ex) when (ex.Message == "Unable to refresh metadata") {
                    // expected
                }
            }
        }

        [Test]
        public async Task FetchOffsetConsumerGroupExistsTest()
        {
            using (var router = new Router(_kafkaUri, new ConnectionFactory(), _config)) {
                await router.TemporaryTopicAsync(async topicName => {
                    var partitionId = 0;
                    var consumerGroup = TestConfig.GroupId();

                    var offset = 5L;

                    await router.CommitTopicOffsetAsync(topicName, partitionId, consumerGroup, offset, CancellationToken.None);
                    var res = await router.GetTopicOffsetAsync(topicName, _partitionId, consumerGroup, CancellationToken.None);

                    Assert.AreEqual(offset, res.Offset);
                });
            }
        }

        [Test]
        public async Task FetchOffsetConsumerGroupArgumentNull([Values(null, "")] string group)
        {
            using (var router = new Router(_kafkaUri, new ConnectionFactory(), _config)) {
                await router.TemporaryTopicAsync(async topicName => {
                    var partitionId = 0;
                    var consumerGroup = TestConfig.GroupId();

                    var offset = 5;

                    await router.CommitTopicOffsetAsync(topicName, partitionId, consumerGroup, offset, CancellationToken.None);
                    Assert.ThrowsAsync<ArgumentNullException>(async () => await router.GetTopicOffsetAsync(topicName, partitionId, group, CancellationToken.None));
                });
            }
        }

        [Test]
        public async Task UpdateOrCreateOffsetConsumerGroupExistsTest()
        {
            using (var router = new Router(_kafkaUri, new ConnectionFactory(), _config)) {
                await router.TemporaryTopicAsync(async topicName => {
                    var partitionId = 0;
                    var consumerGroup = TestConfig.GroupId();

                    var offest = 5;
                    var newOffset = 10;

                    await router.GetTopicOffsetAsync(topicName, partitionId, CancellationToken.None);
                    await router.CommitTopicOffsetAsync(topicName, partitionId, consumerGroup, offest, CancellationToken.None);
                    var res = await router.GetTopicOffsetAsync(topicName, partitionId, consumerGroup, CancellationToken.None);
                    Assert.AreEqual(offest, res.Offset);

                    await router.CommitTopicOffsetAsync(topicName, partitionId, consumerGroup, newOffset, CancellationToken.None);
                    res = await router.GetTopicOffsetAsync(topicName, partitionId, consumerGroup, CancellationToken.None);

                    Assert.AreEqual(newOffset, res.Offset);
                });
            }
        }

        [Test]
        public async Task UpdateOrCreateOffsetPartitionDoesntExistTest()
        {
            using (var router = new Router(_kafkaUri, new ConnectionFactory(), _config)) {
                await router.TemporaryTopicAsync(async topicName => {
                    var partitionId = 100;
                    var consumerGroup = Guid.NewGuid().ToString();

                    var offest = 5;

                    Assert.ThrowsAsync<CachedMetadataException>(async () => await router.CommitTopicOffsetAsync(topicName, partitionId, consumerGroup, offest, CancellationToken.None));
                });
            }
        }

        [Test]
        public async Task UpdateOrCreateOffsetTopicDoesntExistTest()
        {
            using (var router = new Router(_kafkaUri, new ConnectionFactory(), _config, log: TestConfig.Log)) {
                var topicName = TestConfig.TopicName();
                try {
                    await router.SendToAnyAsync(new DeleteTopicsRequest(new [] { topicName }, TimeSpan.FromSeconds(1)), CancellationToken.None);
                } catch (RequestException ex) when (ex.ErrorCode == ErrorResponseCode.TopicAlreadyExists) {
                    // ignore
                }

                var partitionId = 0;
                var consumerGroup = TestConfig.GroupId();

                var offest = 5;
                try {
                    await router.CommitTopicOffsetAsync(topicName, partitionId, consumerGroup, offest, CancellationToken.None);
                    Assert.Fail("should have thrown CachedMetadataException");
                } catch (CachedMetadataException ex) when (ex.Message == "Unable to refresh metadata") {
                    // expected
                }
            }
        }

        [Test]
        public async Task UpdateOrCreateOffsetConsumerGroupArgumentNull([Values(null, "")] string group)
        {
            using (var router = new Router(_kafkaUri, new ConnectionFactory(), _config)) {
                await router.TemporaryTopicAsync(async topicName => {
                    var partitionId = 0;

                    var offest = 5;

                    Assert.ThrowsAsync<ArgumentNullException>(async () => await router.CommitTopicOffsetAsync(topicName, partitionId, group, offest, CancellationToken.None));
                });
            }
        }

        [Test]
        public async Task UpdateOrCreateOffsetNegativeOffsetTest()
        {
            using (var router = new Router(_kafkaUri, new ConnectionFactory(), _config)) {
                await router.TemporaryTopicAsync(async topicName => {
                    var partitionId = 0;
                    var consumerGroup = TestConfig.GroupId();

                    var offest = -5;

                    Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await router.CommitTopicOffsetAsync(topicName, partitionId, consumerGroup, offest, CancellationToken.None));
                });
            }
        }

        [Test]
        public async Task FetchLastOffsetSimpleTest()
        {
            using (var router = new Router(_kafkaUri, new ConnectionFactory(), _config)) {
                await router.TemporaryTopicAsync(async topicName => {
                    var offset = await router.GetTopicOffsetAsync(topicName, _partitionId, CancellationToken.None);

                    Assert.AreNotEqual(-1, offset.Offset);
                });
            }
        }

        [Test]
        public async Task FetchLastOffsetPartitionDoesntExistTest()
        {
            using (var router = new Router(_kafkaUri, new ConnectionFactory(), _config)) {
                await router.TemporaryTopicAsync(async topicName => {
                    var partitionId = 100;


                    Assert.ThrowsAsync<CachedMetadataException>(async () => await router.GetTopicOffsetAsync(topicName, partitionId, CancellationToken.None));
                });
            }
        }

        [Test]
        public async Task FetchLastOffsetTopicDoesntExistTest()
        {
            using (var router = new Router(_kafkaUri, new ConnectionFactory(), _config, log: TestConfig.Log)) {
                var topicName = TestConfig.TopicName();
                try {
                    await router.SendToAnyAsync(new DeleteTopicsRequest(new [] { topicName }, TimeSpan.FromSeconds(1)), CancellationToken.None);
                } catch (RequestException ex) when (ex.ErrorCode == ErrorResponseCode.TopicAlreadyExists) {
                    // ignore
                }

                try {
                    await router.GetTopicOffsetAsync(topicName, _partitionId, CancellationToken.None);
                    Assert.Fail("should have thrown CachedMetadataException");
                } catch (CachedMetadataException ex) when (ex.Message == "Unable to refresh metadata") {
                    // expected
                }
            }
        }

        [Test]
        public async Task JoiningConsumerGroupOnMissingTopicFails()
        {
            using (var router = new Router(_options)) {
                await router.TemporaryTopicAsync(async topicName => {
                    var consumerGroup = TestConfig.GroupId();

                    using (var consumer = new Consumer(router, _consumerConfig, _config.Encoders)) {
                        using (var member = await consumer.JoinConsumerGroupAsync(consumerGroup, new ConsumerProtocolMetadata(topicNames: new[] { topicName }), CancellationToken.None)) {
                            Assert.That(member.GroupId, Is.EqualTo(consumerGroup));
                        }
                    }
                });
            }
        }

        [Test]
        public async Task ConsumerCanJoinConsumerGroup()
        {
            using (var router = new Router(_options)) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var consumer = new Consumer(router, _consumerConfig, _config.Encoders)) {
                        var groupId = TestConfig.GroupId();
                        using (var member = await consumer.JoinConsumerGroupAsync(groupId, new ConsumerProtocolMetadata(topicNames: new[] { TestConfig.TopicName() }), CancellationToken.None)) {
                            Assert.That(member.GroupId, Is.EqualTo(groupId));
                            Assert.That(member.LeaderId, Is.EqualTo(member.MemberId));
                        }
                    }
                });
            }
        }

        #region helpers

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

        #endregion
    }
}