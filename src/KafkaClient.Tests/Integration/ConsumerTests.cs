using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Assignment;
using KafkaClient.Common;
using KafkaClient.Connections;
using KafkaClient.Protocol;
using NUnit.Framework;

#pragma warning disable 1998

namespace KafkaClient.Tests.Integration
{
    [TestFixture]
    public class ConsumerTests
    {
        private readonly KafkaOptions _options;
        private readonly Uri _kafkaUri;
        private const int DefaultMaxMessageSetSize = 4096 * 8;
        private readonly int _partitionId = 0;
        private readonly IConnectionConfiguration _config;
        private readonly IConsumerConfiguration _consumerConfig;

        public ConsumerTests()
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
                        var messageValue = Guid.NewGuid().ToString();
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
                        using (var consumer = new Consumer(router, _consumerConfig)) {
                            var offset = await router.GetTopicOffsetAsync(topicName, _partitionId, CancellationToken.None);

                            // Produce 5 messages
                            var messages = CreateTestMessages(5, 1);
                            await producer.SendMessagesAsync(messages, topicName, _partitionId, new SendMessageConfiguration(ackTimeout: TimeSpan.FromSeconds(3)), CancellationToken.None);

                            // Consume messages, and check
                            var result = await consumer.FetchBatchAsync(offset, 5, CancellationToken.None);
                            CheckMessages(messages, result);
                        }
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
                        using (var consumer = new Consumer(router, _consumerConfig)) {
                            var offset = await router.GetTopicOffsetAsync(topicName, _partitionId, CancellationToken.None);

                            // Produce 5 messages
                            var messages = CreateTestMessages(10, 1);
                            await producer.SendMessagesAsync(messages, topicName, _partitionId, new SendMessageConfiguration(ackTimeout: TimeSpan.FromSeconds(3)), CancellationToken.None);

                            // Consume messages, and check
                            var result = await consumer.FetchBatchAsync(offset, 5, CancellationToken.None);
                            CheckMessages(messages.Take(5).ToList(), result);

                            // Now let's consume again
                            result = await consumer.FetchBatchAsync(offset.TopicName, offset.PartitionId, offset.Offset + 5, 5, CancellationToken.None);
                            CheckMessages(messages.Skip(5).ToList(), result);
                        }
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
                        using (var consumer = new Consumer(router, _consumerConfig)) {
                            var offset = await router.GetTopicOffsetAsync(topicName, _partitionId, CancellationToken.None);

                            // Produce messages
                            var messages = CreateTestMessages(10, 4096);
                            await producer.SendMessagesAsync(messages, topicName, _partitionId, new SendMessageConfiguration(ackTimeout: TimeSpan.FromSeconds(3)), CancellationToken.None);

                            // Consume messages, and check
                            var result = await consumer.FetchBatchAsync(offset, 7, CancellationToken.None);
                            CheckMessages(messages.Take(7).ToList(), result);

                            // Now let's consume again
                            result = await consumer.FetchBatchAsync(offset.TopicName, offset.PartitionId, offset.Offset + 5, 2, CancellationToken.None);
                            CheckMessages(messages.Skip(8).ToList(), result);
                        }
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
                        using (var consumer = new Consumer(router, _consumerConfig)) {
                            var offset = await router.GetTopicOffsetAsync(topicName, _partitionId, CancellationToken.None);

                            // Produce messages
                            var messages = CreateTestMessages(10, 4096);
                            await producer.SendMessagesAsync(messages, topicName, _partitionId, new SendMessageConfiguration(ackTimeout: TimeSpan.FromSeconds(3)), CancellationToken.None);

                            // Consume messages, and check
                            var result = await consumer.FetchBatchAsync(offset, 5, CancellationToken.None);
                            CheckMessages(messages.Take(5).ToList(), result);

                            // Now let's consume again
                            result = await consumer.FetchBatchAsync(offset.TopicName, offset.PartitionId, offset.Offset + 5, 5, CancellationToken.None);
                            CheckMessages(messages.Skip(5).ToList(), result);
                        }
                    }
                });
            }
        }

        [Test]
        public async Task FetchMessagesNoNewMessagesInQueueTest()
        {
            using (var router = new Router(_options)) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var consumer = new Consumer(router, _consumerConfig)) {
                        var offset = await router.GetTopicOffsetAsync(TestConfig.TopicName(), _partitionId, CancellationToken.None);

                        // Now let's consume
                        var result = await consumer.FetchBatchAsync(offset, 5, CancellationToken.None);
                        Assert.AreEqual(0, result.Messages.Count, "Should not get any messages");
                    }
                });
            }
        }

        [Test]
        public async Task FetchMessagesOffsetBiggerThanLastOffsetInQueueTest()
        {
            using (var router = new Router(_kafkaUri, new ConnectionFactory(), _config, log: TestConfig.Log)) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var consumer = new Consumer(router, _consumerConfig)) {
                        var offset = await router.GetTopicOffsetAsync(TestConfig.TopicName(), _partitionId, CancellationToken.None);

                        try {
                            // Now let's consume
                            await consumer.FetchBatchAsync(offset.TopicName, offset.PartitionId, offset.Offset + 1, 5, CancellationToken.None);
                            Assert.Fail("should have thrown FetchOutOfRangeException");
                        } catch (FetchOutOfRangeException ex) when (ex.Message.StartsWith("Kafka returned OffsetOutOfRange for Fetch request")) {
                            Console.WriteLine(ex.ToString());
                        }
                    }
                });
            }
        }

        [Test]
        public async Task FetchMessagesInvalidOffsetTest()
        {
            using (var router = new Router(_kafkaUri, new ConnectionFactory(), _config)) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var consumer = new Consumer(router, _consumerConfig)) {
                        var offset = -1;

                        // Now let's consume
                        Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await consumer.FetchBatchAsync(topicName, _partitionId, offset, 5, CancellationToken.None));
                    }
                });
            }
        }

        [Test]
        public async Task FetchMessagesTopicDoesntExist()
        {
            using (var router = new Router(_kafkaUri, new ConnectionFactory(), _config)) {
                var topicName = TestConfig.TopicName();
                await router.DeleteTopicAsync();
                using (var consumer = new Consumer(router, new ConsumerConfiguration(maxPartitionFetchBytes: DefaultMaxMessageSetSize * 2))) {
                    var offset = 0;

                    // Now let's consume
                    try {
                        await consumer.FetchBatchAsync(topicName, _partitionId, offset, 5, CancellationToken.None);
                        Assert.Fail("should have thrown CachedMetadataException");
                    } catch (CachedMetadataException ex) when (ex.Message.StartsWith("Unable to refresh metadata")) {
                        // expected
                    }
                }
            }
        }

        [Test]
        public async Task FetchMessagesPartitionDoesntExist()
        {
            using (var router = new Router(_kafkaUri, new ConnectionFactory(), _config)) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var consumer = new Consumer(router, new ConsumerConfiguration(maxPartitionFetchBytes: DefaultMaxMessageSetSize * 2))) {
                        var offset = 0;
                        var partitionId = 100;

                        Assert.ThrowsAsync<CachedMetadataException>(
                            async () => await consumer.FetchBatchAsync(topicName, partitionId, offset, 5, CancellationToken.None));
                    }
                });
            }
        }

        [Test]
        public async Task FetchMessagesBufferUnderRunNoMultiplier()
        {
            using (var router = new Router(_options)) {
                await router.TemporaryTopicAsync(async topicName => {
                    var smallMessageSet = 4096 / 2;

                    using (var producer = new Producer(router)) {
                        using (var consumer = new Consumer(router, new ConsumerConfiguration(maxPartitionFetchBytes: smallMessageSet))) {
                            var offset = await router.GetTopicOffsetAsync(topicName, _partitionId, CancellationToken.None);

                            // Creating 5 messages
                            var messages = CreateTestMessages(10, 4096);

                            await producer.SendMessagesAsync(
                                messages, topicName, _partitionId,
                                new SendMessageConfiguration(ackTimeout: TimeSpan.FromSeconds(3)), CancellationToken.None);

                            try {
                                // Now let's consume
                                await consumer.FetchBatchAsync(offset, 5, CancellationToken.None);
                                Assert.Fail("should have thrown BufferUnderRunException");
                            } catch (BufferUnderRunException) {
                                // Console.WriteLine(ex.ToString());
                            }
                        }
                    }
                });
            }
        }

        [Test]
        public async Task FetchMessagesBufferUnderRunWithMultiplier()
        {
            using (var router = new Router(_options)) {
                await router.TemporaryTopicAsync(async topicName => {
                    var smallMessageSet = 4096 / 3;

                    using (var producer = new Producer(router)) {
                        using (var consumer = new Consumer(router, new ConsumerConfiguration(maxPartitionFetchBytes: smallMessageSet, fetchByteMultiplier: 2))) {
                            var offset = await router.GetTopicOffsetAsync(topicName, _partitionId, CancellationToken.None);

                            // Creating 5 messages
                            var messages = CreateTestMessages(10, 4096);

                            await producer.SendMessagesAsync(
                                messages, topicName, _partitionId,
                                new SendMessageConfiguration(ackTimeout: TimeSpan.FromSeconds(3)), CancellationToken.None);

                            // Now let's consume
                            await consumer.FetchBatchAsync(offset, 5, CancellationToken.None);
                        }
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
                    var groupId = Guid.NewGuid().ToString();

                    await router.GetTopicOffsetAsync(topicName, partitionId, groupId, CancellationToken.None);
                });
            }
        }

        [Test]
        public async Task FetchOffsetPartitionDoesntExistTest()
        {
            using (var router = new Router(_kafkaUri, new ConnectionFactory(), _config)) {
                await router.TemporaryTopicAsync(async topicName => {
                    var partitionId = 100;
                    var groupId = TestConfig.GroupId();

                    Assert.ThrowsAsync<CachedMetadataException>(async () => await router.GetTopicOffsetAsync(topicName, partitionId, groupId, CancellationToken.None));
                });
            }
        }

        [Test]
        public async Task FetchOffsetTopicDoesntExistTest()
        {
            using (var router = new Router(_kafkaUri, new ConnectionFactory(), _config)) {
                var topicName = TestConfig.TopicName();
                await router.DeleteTopicAsync();

                var groupId = TestConfig.GroupId();
                try {
                    await router.GetTopicOffsetAsync(topicName, _partitionId, groupId, CancellationToken.None);
                    Assert.Fail("should have thrown CachedMetadataException");
                } catch (CachedMetadataException ex) when (ex.Message.StartsWith("Unable to refresh metadata")) {
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
                    var groupId = TestConfig.GroupId();

                    var offset = 5L;

                    await router.CommitTopicOffsetAsync(topicName, partitionId, groupId, offset, CancellationToken.None);
                    var res = await router.GetTopicOffsetAsync(topicName, _partitionId, groupId, CancellationToken.None);

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
                    var groupId = TestConfig.GroupId();

                    var offset = 5;

                    await router.CommitTopicOffsetAsync(topicName, partitionId, groupId, offset, CancellationToken.None);
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
                    var groupId = TestConfig.GroupId();

                    var offest = 5;
                    var newOffset = 10;

                    await router.GetTopicOffsetAsync(topicName, partitionId, CancellationToken.None);
                    await router.CommitTopicOffsetAsync(topicName, partitionId, groupId, offest, CancellationToken.None);
                    var res = await router.GetTopicOffsetAsync(topicName, partitionId, groupId, CancellationToken.None);
                    Assert.AreEqual(offest, res.Offset);

                    await router.CommitTopicOffsetAsync(topicName, partitionId, groupId, newOffset, CancellationToken.None);
                    res = await router.GetTopicOffsetAsync(topicName, partitionId, groupId, CancellationToken.None);

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
                    var groupId = Guid.NewGuid().ToString();

                    var offest = 5;

                    Assert.ThrowsAsync<CachedMetadataException>(async () => await router.CommitTopicOffsetAsync(topicName, partitionId, groupId, offest, CancellationToken.None));
                });
            }
        }

        [Test]
        public async Task UpdateOrCreateOffsetTopicDoesntExistTest()
        {
            using (var router = new Router(_kafkaUri, new ConnectionFactory(), _config, log: TestConfig.Log)) {
                var topicName = TestConfig.TopicName();
                await router.DeleteTopicAsync();

                var partitionId = 0;
                var groupId = TestConfig.GroupId();

                var offest = 5;
                try {
                    await router.CommitTopicOffsetAsync(topicName, partitionId, groupId, offest, CancellationToken.None);
                    Assert.Fail("should have thrown CachedMetadataException");
                } catch (CachedMetadataException ex) when (ex.Message.StartsWith("Unable to refresh metadata")) {
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
                    var groupId = TestConfig.GroupId();

                    var offest = -5;

                    Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await router.CommitTopicOffsetAsync(topicName, partitionId, groupId, offest, CancellationToken.None));
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
                await router.DeleteTopicAsync();

                try {
                    await router.GetTopicOffsetAsync(topicName, _partitionId, CancellationToken.None);
                    Assert.Fail("should have thrown CachedMetadataException");
                } catch (CachedMetadataException ex) when (ex.Message.StartsWith("Unable to refresh metadata")) {
                    // expected
                }
            }
        }

        [Test]
        public async Task ConsumeByOffsetShouldGetSameMessageProducedAtSameOffset()
        {
            long offsetResponse;
            var messge = Guid.NewGuid();

            using (var router = new Router(TestConfig.IntegrationUri, log: TestConfig.Log)) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        var responseAckLevel1 = await producer.SendMessageAsync(new Message(messge.ToString()), topicName, 0, new SendMessageConfiguration(acks: 1), CancellationToken.None);
                        offsetResponse = responseAckLevel1.Offset;
                    }
                    using (var consumer = new Consumer(router, new ConsumerConfiguration(maxServerWait: TimeSpan.Zero))) {
                        var result = await consumer.FetchBatchAsync(topicName, 0, offsetResponse, 1, CancellationToken.None);
                        Assert.AreEqual(messge.ToString(), result.Messages[0].Value.ToUtf8String());
                    }
                });
            }
        }

        [Test]
        public async Task ConsumerShouldBeAbleToSeekBackToEarlierOffset([Values(20)] int sends, [Values(1, 10)] int messagesPerSend)
        {
            var totalMessages = sends * messagesPerSend;

            var testId = Guid.NewGuid().ToString();

            using (var router = new Router(TestConfig.IntegrationUri, log: TestConfig.Log )) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        var offset = await producer.Router.GetTopicOffsetAsync(topicName, 0, CancellationToken.None);

                        for (var i = 0; i < sends; i++) {
                            if (messagesPerSend == 1) {
                                await producer.SendMessageAsync(new Message(i.ToString(), testId), topicName, 0, CancellationToken.None);
                            } else {
                                var current = i * messagesPerSend;
                                var messages = messagesPerSend.Repeat(_ => new Message((current + _).ToString(), testId)).ToList();
                                await producer.SendMessagesAsync(messages, topicName, 0, CancellationToken.None);
                            }
                        }

                        using (var consumer = new Consumer(router, new ConsumerConfiguration(maxServerWait: TimeSpan.Zero))) {
                            var results1 = await consumer.FetchBatchAsync(offset, totalMessages, CancellationToken.None);
                            TestConfig.Log.Info(() => LogEvent.Create($"Message order:  {string.Join(", ", results1.Messages.Select(x => x.Value.ToUtf8String()).ToList())}"));

                            var results2 = await consumer.FetchBatchAsync(offset, totalMessages, CancellationToken.None);
                            TestConfig.Log.Info(() => LogEvent.Create($"Message order:  {string.Join(", ", results2.Messages.Select(x => x.Value.ToUtf8String()).ToList())}"));

                            Assert.That(results1.Messages.Count, Is.EqualTo(results2.Messages.Count));
                            Assert.That(results1.Messages.Count, Is.EqualTo(totalMessages));
                            Assert.That(results1.Messages.Select(x => x.Value.ToUtf8String()).ToList(), Is.EqualTo(results2.Messages.Select(x => x.Value.ToUtf8String()).ToList()), "Expected the message list in the correct order.");
                        }
                    }
                });
            }
        }

        [Test]
        public async Task ConsumerShouldBeAbleToGetCurrentOffsetInformation()
        {
            var totalMessages = 20;
            var expected = totalMessages.Repeat(i => i.ToString()).ToList();
            var testId = Guid.NewGuid().ToString();

            using (var router = new Router(TestConfig.IntegrationUri, log: TestConfig.Log )) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        var offset = await producer.Router.GetTopicOffsetAsync(topicName, 0, CancellationToken.None);

                        for (var i = 0; i < totalMessages; i++) {
                            await producer.SendMessageAsync(new Message(i.ToString(), testId), topicName, 0, CancellationToken.None);
                        }

                        using (var consumer = new Consumer(router, new ConsumerConfiguration(maxServerWait: TimeSpan.Zero))) {
                            var results = await consumer.FetchBatchAsync(offset, totalMessages, CancellationToken.None);
                            TestConfig.Log.Info(() => LogEvent.Create($"Message order:  {string.Join(", ", results.Messages.Select(x => x.Value.ToUtf8String()).ToList())}"));

                            Assert.That(results.Messages.Count, Is.EqualTo(totalMessages));
                            Assert.That(results.Messages.Select(x => x.Value.ToUtf8String()).ToList(), Is.EqualTo(expected), "Expected the message list in the correct order.");

                            var newOffset = await producer.Router.GetTopicOffsetAsync(offset.TopicName, offset.PartitionId, CancellationToken.None);
                            Assert.That(newOffset.Offset - offset.Offset, Is.EqualTo(totalMessages));
                        }
                    }
                });
            }
        }

        [Test]
        [TestCase(1, 200)]
        [TestCase(1000, 500)]
        public async Task ConsumerShouldConsumeInSameOrderAsProduced(int totalMessages, int timeoutInMs)
        {
            var expected = totalMessages.Repeat(i => i.ToString()).ToList();

            using (var router = new Router(TestConfig.IntegrationUri, log: TestConfig.Log)) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router, new ProducerConfiguration(batchSize: totalMessages / 10, batchMaxDelay: TimeSpan.FromMilliseconds(25)))) {
                        var offset = await producer.Router.GetTopicOffsetAsync(TestConfig.TopicName(), 0, CancellationToken.None);

                        var stopwatch = new Stopwatch();
                        stopwatch.Start();
                        var sendList = new List<Task>(totalMessages);
                        for (var i = 0; i < totalMessages; i++) {
                            var sendTask = producer.SendMessageAsync(new Message(i.ToString()), offset.TopicName, offset.PartitionId, CancellationToken.None);
                            sendList.Add(sendTask);
                        }
                        var maxTimeToRun = TimeSpan.FromMilliseconds(timeoutInMs);
                        var doneSend = Task.WhenAll(sendList.ToArray());
                        await Task.WhenAny(doneSend, Task.Delay(maxTimeToRun));
                        stopwatch.Stop();
                        if (!doneSend.IsCompleted) {
                            var completed = sendList.Count(t => t.IsCompleted);
                            Assert.Inconclusive($"Only finished sending {completed} of {totalMessages} in {timeoutInMs} ms.");
                        }
                        await doneSend;
                        TestConfig.Log.Info(() => LogEvent.Create($">> done send, time Milliseconds:{stopwatch.ElapsedMilliseconds}"));
                        stopwatch.Restart();

                        using (var consumer = new Consumer(router, new ConsumerConfiguration(maxServerWait: TimeSpan.Zero))) {
                            var fetched = ImmutableList<Message>.Empty;
                            stopwatch.Restart();
                            while (fetched.Count < totalMessages) {
                                var doneFetch = consumer.FetchBatchAsync(offset.TopicName, offset.PartitionId, offset.Offset + fetched.Count, totalMessages, CancellationToken.None);
                                var delay = Task.Delay((int) Math.Max(0, maxTimeToRun.TotalMilliseconds - stopwatch.ElapsedMilliseconds));
                                await Task.WhenAny(doneFetch, delay);
                                if (delay.IsCompleted && !doneFetch.IsCompleted) {
                                    Assert.Fail($"Received {fetched.Count} of {totalMessages} in {timeoutInMs} ms.");
                                }
                                var results = await doneFetch;
                                fetched = fetched.AddRange(results.Messages);
                            }
                            stopwatch.Stop();
                            TestConfig.Log.Info(() => LogEvent.Create($">> done Consume, time Milliseconds:{stopwatch.ElapsedMilliseconds}"));

                            Assert.That(fetched.Select(x => x.Value.ToUtf8String()).ToList(), Is.EqualTo(expected), "Expected the message list in the correct order.");
                            Assert.That(fetched.Count, Is.EqualTo(totalMessages));
                        }
                    }
                });
            }
        }

        //[Test]
        //public async Task ConsumerShouldNotLoseMessageWhenBlocked()
        //{
        //    var testId = Guid.NewGuid().ToString();

        //    using (var router = new Router(new KafkaOptions(TestConfig.IntegrationUri))) {
        //        await router.TemporaryTopicAsync(async topicName => {
        //            using (var producer = new Producer(router)) {
        //                var offsets = await producer.Router.GetTopicOffsetsAsync(topicName, CancellationToken.None);

        //                //create consumer with buffer size of 1 (should block upstream)
        //                using (var consumer = new OldConsumer(new ConsumerOptions(topicName, router) { ConsumerBufferSize = 1, MaxWaitTimeForMinimumBytes = TimeSpan.Zero },
        //                      offsets.Select(x => new OffsetPosition(x.PartitionId, x.Offset)).ToArray()))
        //                {
        //                    for (var i = 0; i < 20; i++)
        //                    {
        //                        await producer.SendMessageAsync(new Message(i.ToString(), testId), topicName, CancellationToken.None);
        //                    }

        //                    for (var i = 0; i < 20; i++)
        //                    {
        //                        var result = consumer.Consume().Take(1).First();
        //                        Assert.That(result.Key.ToUtf8String(), Is.EqualTo(testId));
        //                        Assert.That(result.Value.ToUtf8String(), Is.EqualTo(i.ToString()));
        //                    }
        //                }
        //            }
        //        });
        //    }
        //}

        //[Test]
        //public async Task ConsumerShouldMoveToNextAvailableOffsetWhenQueryingForNextMessage()
        //{
        //    const int expectedCount = 1000;
        //    var options = new KafkaOptions(TestConfig.IntegrationUri);

        //    using (var router = new Router(options)) {
        //        await router.TemporaryTopicAsync(async topicName => {
        //            using (var producer = new Producer(router)) {
        //                //get current offset and reset consumer to top of log
        //                var offsets = await producer.Router.GetTopicOffsetsAsync(topicName, CancellationToken.None).ConfigureAwait(false);

        //                using (var consumerRouter = new Router(options))
        //                using (var consumer = new OldConsumer(new ConsumerOptions(topicName, consumerRouter) { MaxWaitTimeForMinimumBytes = TimeSpan.Zero },
        //                     offsets.Select(x => new OffsetPosition(x.PartitionId, x.Offset)).ToArray()))
        //                {
        //                    Console.WriteLine("Sending {0} test messages", expectedCount);
        //                    var response = await producer.SendMessagesAsync(Enumerable.Range(0, expectedCount).Select(x => new Message(x.ToString())), topicName, CancellationToken.None);

        //                    Assert.That(response.Any(x => x.ErrorCode != (int)ErrorResponseCode.None), Is.False, "Error occured sending test messages to server.");

        //                    var stream = consumer.Consume();

        //                    Console.WriteLine("Reading message back out from consumer.");
        //                    var data = stream.Take(expectedCount).ToList();

        //                    var consumerOffset = consumer.GetOffsetPosition().OrderBy(x => x.PartitionId).ToList();

        //                    var serverOffset = await producer.Router.GetTopicOffsetsAsync(topicName, CancellationToken.None).ConfigureAwait(false);
        //                    var positionOffset = serverOffset.Select(x => new OffsetPosition(x.PartitionId, x.Offset))
        //                        .OrderBy(x => x.PartitionId)
        //                        .ToList();

        //                    Assert.That(consumerOffset, Is.EqualTo(positionOffset), "The consumerOffset position should match the server offset position.");
        //                    Assert.That(data.Count, Is.EqualTo(expectedCount), "We should have received 2000 messages from the server.");
        //                }
        //            }
        //        });
        //    }
        //}

        [Test]
        public async Task JoiningConsumerGroupOnMissingTopicFails()
        {
            using (var router = new Router(_options)) {
                await router.TemporaryTopicAsync(async topicName => {
                    var groupId = TestConfig.GroupId();

                    using (var consumer = new Consumer(router, _consumerConfig, _config.Encoders)) {
                        using (var member = await consumer.JoinConsumerGroupAsync(groupId, new ConsumerProtocolMetadata(topicName), CancellationToken.None)) {
                            Assert.That(member.GroupId, Is.EqualTo(groupId));
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
                        using (var member = await consumer.JoinConsumerGroupAsync(groupId, new ConsumerProtocolMetadata(TestConfig.TopicName()), CancellationToken.None)) {
                            Assert.That(member.GroupId, Is.EqualTo(groupId));
                            Assert.That(member.IsLeader, Is.True);
                        }
                    }
                });
            }
        }

        [Test]
        [TestCase(1000, 100)]
        [TestCase(1000, 500)]
        public async Task CanConsumeFromGroup(int totalMessages, int count)
        {
            using (var router = new Router(TestConfig.IntegrationUri, log: TestConfig.Log)) {
                await router.TemporaryTopicAsync(async topicName => {
                    var groupId = TestConfig.GroupId();

                    using (var producer = new Producer(router, new ProducerConfiguration(batchSize: totalMessages / 10, batchMaxDelay: TimeSpan.FromMilliseconds(25)))) {
                        var offset = await router.GetTopicOffsetAsync(topicName, 0, CancellationToken.None);
                        var groupOffset = await router.GetTopicOffsetAsync(topicName, 0, groupId, CancellationToken.None);

                        var missingMessages = Math.Max(0, totalMessages + groupOffset.Offset - offset.Offset);
                        if (missingMessages > 0) {
                            var messages = new List<Message>();
                            for (var i = 0; i < missingMessages; i++) {
                                messages.Add(new Message(i.ToString()));
                            }
                            await producer.SendMessagesAsync(messages, topicName, 0, CancellationToken.None);
                        }
                    }

                    var fetched = 0;
                    using (var consumer = new Consumer(router, _consumerConfig, _config.Encoders)) {
                        using (var member = await consumer.JoinConsumerGroupAsync(groupId, new ConsumerProtocolMetadata(topicName), CancellationToken.None)) {
                            while (fetched < totalMessages) {
                                var fetchTasks = new List<Task>();
                                for (var batch = await member.FetchBatchAsync(count, CancellationToken.None); batch != MessageBatch.Empty; batch = await member.FetchBatchAsync(count, CancellationToken.None)) {
                                    var currentBatch = batch;
                                    fetchTasks.Add(Task.Run(
                                        async () =>
                                        {
                                            foreach (var message in currentBatch.Messages) {
                                                await Task.Delay(5);
                                                currentBatch.MarkSuccessful(message);
                                                Interlocked.Increment(ref fetched);
                                            }
                                            while (fetched < totalMessages) {
                                                currentBatch = await currentBatch.FetchNextAsync(count, CancellationToken.None);
                                                if (currentBatch.Messages.Count == 0) break;
                                                foreach (var message in currentBatch.Messages) {
                                                    await Task.Delay(5);
                                                    currentBatch.MarkSuccessful(message);
                                                    Interlocked.Increment(ref fetched);
                                                }
                                            }
                                        }));
                                }
                                await Task.WhenAll(fetchTasks);
                            }
                        }
                    }
                    Assert.That(fetched, Is.EqualTo(totalMessages));
                });
            }
        }

        #region helpers

        private void CheckMessages(List<Message> expected, IConsumerMessageBatch actual)
        {
            Assert.AreEqual(expected.Count(), actual.Messages.Count(), "Didn't get all messages");

            foreach (var message in expected)
            {
                Assert.IsTrue(actual.Messages.Any(m => m.Value.SequenceEqual(message.Value)), "Didn't get the same messages");
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