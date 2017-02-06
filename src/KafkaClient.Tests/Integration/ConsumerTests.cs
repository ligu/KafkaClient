﻿using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Assignment;
using KafkaClient.Common;
using KafkaClient.Protocol;
using NUnit.Framework;

#pragma warning disable 1998

namespace KafkaClient.Tests.Integration
{
    [TestFixture]
    public class ConsumerTests
    {
        [Test]
        public async Task CanFetch()
        {
            const int partitionId = 0;
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
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
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        using (var consumer = new Consumer(router, TestConfig.IntegrationOptions.ConsumerConfiguration)) {
                            var offset = await router.GetTopicOffsetAsync(topicName, 0, CancellationToken.None);

                            // Produce 5 messages
                            var messages = CreateTestMessages(5, 1);
                            await producer.SendMessagesAsync(messages, topicName, 0, new SendMessageConfiguration(ackTimeout: TimeSpan.FromSeconds(3)), CancellationToken.None);

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
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        using (var consumer = new Consumer(router, TestConfig.IntegrationOptions.ConsumerConfiguration)) {
                            var offset = await router.GetTopicOffsetAsync(topicName, 0, CancellationToken.None);

                            // Produce 5 messages
                            var messages = CreateTestMessages(10, 1);
                            await producer.SendMessagesAsync(messages, topicName, 0, new SendMessageConfiguration(ackTimeout: TimeSpan.FromSeconds(3)), CancellationToken.None);

                            // Consume messages, and check
                            var result = await consumer.FetchBatchAsync(offset, 5, CancellationToken.None);
                            CheckMessages(messages.Take(5).ToList(), result);

                            // Now let's consume again
                            result = await consumer.FetchBatchAsync(offset.topic, offset.partition_id, offset.Offset + 5, CancellationToken.None, 5);
                            CheckMessages(messages.Skip(5).ToList(), result);
                        }
                    }
                });
            }
        }

        [Test]
        public async Task FetchMessagesCacheContainsNoneOfRequestTest()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        using (var consumer = new Consumer(router, TestConfig.IntegrationOptions.ConsumerConfiguration)) {
                            var offset = await router.GetTopicOffsetAsync(topicName, 0, CancellationToken.None);

                            // Produce messages
                            var messages = CreateTestMessages(10, 4096);
                            await producer.SendMessagesAsync(messages, topicName, 0, new SendMessageConfiguration(ackTimeout: TimeSpan.FromSeconds(3)), CancellationToken.None);

                            // Consume messages, and check
                            var result = await consumer.FetchBatchAsync(offset, 7, CancellationToken.None);
                            CheckMessages(messages.Take(7).ToList(), result);

                            // Now let's consume again
                            result = await consumer.FetchBatchAsync(offset.topic, offset.partition_id, offset.Offset + 5, CancellationToken.None, 2);
                            CheckMessages(messages.Skip(8).ToList(), result);
                        }
                    }
                });
            }
        }

        [Test]
        public async Task FetchMessagesCacheContainsPartOfRequestTest()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        using (var consumer = new Consumer(router, TestConfig.IntegrationOptions.ConsumerConfiguration)) {
                            var offset = await router.GetTopicOffsetAsync(topicName, 0, CancellationToken.None);

                            // Produce messages
                            var messages = CreateTestMessages(10, 4096);
                            await producer.SendMessagesAsync(messages, topicName, 0, new SendMessageConfiguration(ackTimeout: TimeSpan.FromSeconds(3)), CancellationToken.None);

                            // Consume messages, and check
                            var result = await consumer.FetchBatchAsync(offset, 5, CancellationToken.None);
                            CheckMessages(messages.Take(5).ToList(), result);

                            // Now let's consume again
                            result = await consumer.FetchBatchAsync(offset.topic, offset.partition_id, offset.Offset + 5, CancellationToken.None, 5);
                            CheckMessages(messages.Skip(5).ToList(), result);
                        }
                    }
                });
            }
        }

        [Test]
        public async Task FetchMessagesNoNewMessagesInQueueTest()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var consumer = new Consumer(router, TestConfig.IntegrationOptions.ConsumerConfiguration)) {
                        var offset = await router.GetTopicOffsetAsync(TestConfig.TopicName(), 0, CancellationToken.None);

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
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var consumer = new Consumer(router, TestConfig.IntegrationOptions.ConsumerConfiguration)) {
                        var offset = await router.GetTopicOffsetAsync(TestConfig.TopicName(), 0, CancellationToken.None);

                        await AssertAsync.Throws<FetchOutOfRangeException>(
                            () => consumer.FetchBatchAsync(offset.topic, offset.partition_id, offset.Offset + 1, CancellationToken.None, 5),
                            ex => ex.Message.StartsWith("kafka1:9092 returned OFFSET_OUT_OF_RANGE for Fetch request"));
                    }
                });
            }
        }

        [Test]
        public async Task FetchMessagesInvalidOffsetTest()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var consumer = new Consumer(router, TestConfig.IntegrationOptions.ConsumerConfiguration)) {
                        var offset = -1;

                        // Now let's consume
                        await AssertAsync.Throws<ArgumentOutOfRangeException>(() => consumer.FetchBatchAsync(topicName, 0, offset, CancellationToken.None, 5));
                    }
                });
            }
        }

        [Test]
        public async Task FetchMessagesTopicDoesntExist()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                var topicName = TestConfig.TopicName();
                await router.DeleteTopicAsync();
                using (var consumer = new Consumer(router, new ConsumerConfiguration(maxPartitionFetchBytes: TestConfig.IntegrationOptions.ConsumerConfiguration.MaxFetchBytes * 2))) {
                    var offset = 0;

                    await AssertAsync.Throws<RoutingException>(
                        () => consumer.FetchBatchAsync(topicName, 0, offset, CancellationToken.None, 5),
                        ex => ex.Message.StartsWith($"The topic ({topicName}) has no partitionId {0} defined."));
                }
            }
        }

        [Test]
        public async Task FetchMessagesPartitionDoesntExist()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var consumer = new Consumer(router, new ConsumerConfiguration(maxPartitionFetchBytes: TestConfig.IntegrationOptions.ConsumerConfiguration.MaxFetchBytes * 2))) {
                        var offset = 0;
                        var partitionId = 100;

                        await AssertAsync.Throws<RoutingException>(
                            () => consumer.FetchBatchAsync(topicName, partitionId, offset, CancellationToken.None, 5),
                            ex => ex.Message.StartsWith($"The topic ({topicName}) has no partitionId {partitionId} defined."));
                    }
                });
            }
        }

        [Test]
        public async Task FetchMessagesBufferUnderRunNoMultiplier()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    var smallMessageSet = 4096 / 2;

                    using (var producer = new Producer(router)) {
                        using (var consumer = new Consumer(router, new ConsumerConfiguration(maxPartitionFetchBytes: smallMessageSet, fetchByteMultiplier: 1))) {
                            var offset = await router.GetTopicOffsetAsync(topicName, 0, CancellationToken.None);

                            // Creating 5 messages
                            var messages = CreateTestMessages(10, 4096);

                            await producer.SendMessagesAsync(
                                messages, topicName, 0,
                                new SendMessageConfiguration(ackTimeout: TimeSpan.FromSeconds(3)), CancellationToken.None);

                            await AssertAsync.Throws<BufferUnderRunException>(() => consumer.FetchBatchAsync(offset, 5, CancellationToken.None));
                        }
                    }
                });
            }
        }

        [Test]
        public async Task FetchMessagesBufferUnderRunWithMultiplier()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    var smallMessageSet = 4096 / 3;

                    using (var producer = new Producer(router)) {
                        using (var consumer = new Consumer(router, new ConsumerConfiguration(maxPartitionFetchBytes: smallMessageSet, fetchByteMultiplier: 2))) {
                            var offset = await router.GetTopicOffsetAsync(topicName, 0, CancellationToken.None);

                            // Creating 5 messages
                            var messages = CreateTestMessages(10, 4096);

                            await producer.SendMessagesAsync(
                                messages, topicName, 0,
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
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
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
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    var partitionId = 100;
                    var groupId = TestConfig.GroupId();

                    try {
                        await router.GetTopicOffsetAsync(topicName, partitionId, groupId, CancellationToken.None);
                        Assert.Fail("should have thrown CachedMetadataException");
                    } catch (RoutingException ex) when (ex.Message.StartsWith($"The topic ({topicName}) has no partitionId {partitionId} defined.")) {
                        // expected
                    }
                });
            }
        }

        [Test]
        public async Task FetchOffsetTopicDoesntExistTest()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                var topicName = TestConfig.TopicName();
                await router.DeleteTopicAsync();

                var groupId = TestConfig.GroupId();
                try {
                    await router.GetTopicOffsetAsync(topicName, 0, groupId, CancellationToken.None);
                    Assert.Fail("should have thrown CachedMetadataException");
                } catch (RoutingException ex) when (ex.Message.StartsWith($"The topic ({topicName}) has no partitionId {0} defined.")) {
                    // expected
                }
            }
        }

        [Test]
        public async Task FetchOffsetConsumerGroupExistsTest()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    var partitionId = 0;
                    var groupId = TestConfig.GroupId();

                    var offset = 5L;

                    await router.CommitTopicOffsetAsync(topicName, partitionId, groupId, offset, CancellationToken.None);
                    var res = await router.GetTopicOffsetAsync(topicName, 0, groupId, CancellationToken.None);

                    Assert.AreEqual(offset, res.Offset);
                });
            }
        }

        [Test]
        public async Task FetchOffsetConsumerGroupArgumentNull([Values(null, "")] string group)
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
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
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
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
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    var partitionId = 100;
                    var groupId = Guid.NewGuid().ToString();

                    var offest = 5;
                    try {
                        await router.CommitTopicOffsetAsync(topicName, partitionId, groupId, offest, CancellationToken.None);
                        Assert.Fail("should have thrown CachedMetadataException");
                    } catch (RoutingException ex) when (ex.Message.StartsWith($"The topic ({topicName}) has no partitionId {partitionId} defined.")) {
                        // expected
                    }
                });
            }
        }

        [Test]
        public async Task UpdateOrCreateOffsetTopicDoesntExistTest()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                var topicName = TestConfig.TopicName();
                await router.DeleteTopicAsync();

                var partitionId = 0;
                var groupId = TestConfig.GroupId();

                var offest = 5;
                try {
                    await router.CommitTopicOffsetAsync(topicName, partitionId, groupId, offest, CancellationToken.None);
                    Assert.Fail("should have thrown CachedMetadataException");
                } catch (RoutingException ex) when (ex.Message.StartsWith($"The topic ({topicName}) has no partitionId {0} defined.")) {
                    // expected
                }
            }
        }

        [Test]
        public async Task UpdateOrCreateOffsetConsumerGroupArgumentNull([Values(null, "")] string group)
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
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
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
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
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    var offset = await router.GetTopicOffsetAsync(topicName, 0, CancellationToken.None);

                    Assert.AreNotEqual(-1, offset.Offset);
                });
            }
        }

        [Test]
        public async Task FetchLastOffsetPartitionDoesntExistTest()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    var partitionId = 100;

                    try {
                        await router.GetTopicOffsetAsync(topicName, partitionId, CancellationToken.None);
                        Assert.Fail("should have thrown CachedMetadataException");
                    } catch (RoutingException ex) when (ex.Message.StartsWith($"The topic ({topicName}) has no partitionId {partitionId} defined.")) {
                        // expected
                    }
                });
            }
        }

        [Test]
        public async Task FetchLastOffsetTopicDoesntExistTest()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                var topicName = TestConfig.TopicName();
                await router.DeleteTopicAsync();

                try {
                    await router.GetTopicOffsetAsync(topicName, 0, CancellationToken.None);
                    Assert.Fail("should have thrown CachedMetadataException");
                } catch (RoutingException ex) when (ex.Message.StartsWith($"The topic ({topicName}) has no partitionId {0} defined.")) {
                    // expected
                }
            }
        }

        [Test]
        public async Task ConsumeByOffsetShouldGetSameMessageProducedAtSameOffset()
        {
            long offsetResponse;
            var messge = Guid.NewGuid();

            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        var responseAckLevel1 = await producer.SendMessageAsync(new Message(messge.ToString()), topicName, 0, new SendMessageConfiguration(acks: 1), CancellationToken.None);
                        offsetResponse = responseAckLevel1.Offset;
                    }
                    using (var consumer = new Consumer(router, new ConsumerConfiguration(maxServerWait: TimeSpan.Zero))) {
                        var result = await consumer.FetchBatchAsync(topicName, 0, offsetResponse, CancellationToken.None, 1);
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

            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
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

            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
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

                            var newOffset = await producer.Router.GetTopicOffsetAsync(offset.topic, offset.partition_id, CancellationToken.None);
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

            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router, new ProducerConfiguration(batchSize: totalMessages / 10, batchMaxDelay: TimeSpan.FromMilliseconds(25)))) {
                        var offset = await producer.Router.GetTopicOffsetAsync(TestConfig.TopicName(), 0, CancellationToken.None);

                        var stopwatch = new Stopwatch();
                        stopwatch.Start();
                        var sendList = new List<Task>(totalMessages);
                        for (var i = 0; i < totalMessages; i++) {
                            var sendTask = producer.SendMessageAsync(new Message(i.ToString()), offset.topic, offset.partition_id, CancellationToken.None);
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
                                var doneFetch = consumer.FetchBatchAsync(offset.topic, offset.partition_id, offset.Offset + fetched.Count, CancellationToken.None, totalMessages);
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

        //    using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
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
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    var groupId = TestConfig.GroupId();

                    using (var consumer = new Consumer(router, TestConfig.IntegrationOptions.ConsumerConfiguration, TestConfig.IntegrationOptions.ConnectionConfiguration.Encoders)) {
                        using (var member = await consumer.JoinConsumerGroupAsync(groupId, new ConsumerProtocolMetadata(topicName), CancellationToken.None)) {
                            Assert.That(member.group_id, Is.EqualTo(groupId));
                        }
                    }
                });
            }
        }

        [Test]
        public async Task ConsumerCanJoinConsumerGroup()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var consumer = new Consumer(router, TestConfig.IntegrationOptions.ConsumerConfiguration, TestConfig.IntegrationOptions.ConnectionConfiguration.Encoders)) {
                        var groupId = TestConfig.GroupId();
                        using (var member = await consumer.JoinConsumerGroupAsync(groupId, new ConsumerProtocolMetadata(TestConfig.TopicName()), CancellationToken.None)) {
                            Assert.That(member.group_id, Is.EqualTo(groupId));
                            Assert.That(member.IsLeader, Is.True);
                        }
                    }
                });
            }
        }

        private static async Task ProduceMessages(Router router, string topicName, string groupId, int totalMessages, IEnumerable<int> partitionIds = null)
        {
            var producer = new Producer(router, new ProducerConfiguration(batchSize: totalMessages / 10, batchMaxDelay: TimeSpan.FromMilliseconds(25), stopTimeout: TimeSpan.FromMilliseconds(50)));
            await producer.UsingAsync(async () => {
                var offsets = await router.GetTopicOffsetsAsync(topicName, CancellationToken.None);
                await router.GetGroupServerIdAsync(groupId, CancellationToken.None);
                foreach (var partitionId in partitionIds ?? new [] { 0 }) {
                    var offset = offsets.SingleOrDefault(o => o.partition_id == partitionId);
                    //await router.SendAsync(new GroupCoordinatorRequest(groupId), topicName, partitionId, CancellationToken.None).ConfigureAwait(false);
                    var groupOffset = await router.GetTopicOffsetAsync(topicName, partitionId, groupId, CancellationToken.None);

                    var missingMessages = Math.Max(0, totalMessages + groupOffset.Offset - offset.Offset + 1);
                    if (missingMessages > 0)
                    {
                        var messages = new List<Message>();
                        for (var i = 0; i < missingMessages; i++) {
                            messages.Add(new Message(i.ToString()));
                        }
                        await producer.SendMessagesAsync(messages, topicName, partitionId, CancellationToken.None);
                    }
                }
            });
        }

        [Test]
        [TestCase(1, 100)]
        [TestCase(2, 100)]
        [TestCase(10, 500)]
        public async Task CanConsumeFromGroup(int members, int batchSize)
        {
            var cancellation = new CancellationTokenSource();
            var totalMessages = 1000;
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName =>
                {
                    var groupId = TestConfig.GroupId();

                    await ProduceMessages(router, topicName, groupId, totalMessages, Enumerable.Range(0, members));

                    var fetched = 0;
                    var consumer = new Consumer(router, TestConfig.IntegrationOptions.ConsumerConfiguration, TestConfig.IntegrationOptions.ConnectionConfiguration.Encoders);
                    var tasks = new List<Task>();
                    for (var index = 0; index < members; index++) {
                        tasks.Add(Task.Run(async () => {
                            var member = await consumer.JoinConsumerGroupAsync(groupId, new ConsumerProtocolMetadata(topicName), cancellation.Token);
                            await member.UsingAsync(async () => {
                                await member.FetchUntilDisposedAsync(async (batch, token) => {
                                    router.Log.Info(() => LogEvent.Create($"Member {member.member_id} starting batch of {batch.Messages.Count}"));
                                    foreach (var message in batch.Messages) {
                                        await Task.Delay(1); // do the work...
                                        batch.MarkSuccessful(message);
                                        if (Interlocked.Increment(ref fetched) >= totalMessages) {
                                            cancellation.Cancel();
                                            break;
                                        }
                                    }
                                    router.Log.Info(() => LogEvent.Create($"Member {member.member_id} finished batch size {batch.Messages.Count} ({fetched} of {totalMessages})"));
                                }, cancellation.Token, batchSize);
                            });
                        }, CancellationToken.None));
                    }
//                    await Task.WhenAny(Task.WhenAll(tasks), Task.Delay(TimeSpan.FromMinutes(1)));
                    await Task.WhenAll(tasks);
                    Assert.That(fetched, Is.AtLeast(totalMessages));
                }, 10);
            }
        }
        
        [Test]
        [TestCase(2, 2)]
        [TestCase(5, 5)]
        public async Task CanConsumeFromMultipleGroups(int groups, int members)
        {
            using (var timed = new TimedCancellation(CancellationToken.None, TimeSpan.FromMinutes(1))) {
                var batchSize = 50;
                var totalMessages = 200;
                using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                    await router.TemporaryTopicAsync(async topicName =>
                    {
                        var allFetched = 0;
                        var tasks = new List<Task>();
                        var groupIdPrefix = TestConfig.GroupId();
                        for (var group = 0; group < groups; group++) {
                            var groupId = $"{groupIdPrefix}.{group}";
                            var groupFetched = 0;
                            await ProduceMessages(router, topicName, groupId, totalMessages, Enumerable.Range(0, members));

                            var cancellation = new CancellationTokenSource();
                            var consumer = new Consumer(router, TestConfig.IntegrationOptions.ConsumerConfiguration, TestConfig.IntegrationOptions.ConnectionConfiguration.Encoders);
                            for (var index = 0; index < members; index++) {
                                tasks.Add(Task.Run(async () => {
                                    using (var merged = CancellationTokenSource.CreateLinkedTokenSource(timed.Token, cancellation.Token)) {
                                        var member = await consumer.JoinConsumerGroupAsync(groupId, new ConsumerProtocolMetadata(topicName), merged.Token);
                                        await member.UsingAsync(async () => {
                                            await member.FetchUntilDisposedAsync(async (batch, token) => {
                                                router.Log.Info(() => LogEvent.Create($"Member {member.member_id} starting batch of {batch.Messages.Count}"));
                                                foreach (var message in batch.Messages) {
                                                    await Task.Delay(1); // do the work...
                                                    batch.MarkSuccessful(message);
                                                    Interlocked.Increment(ref allFetched);
                                                    if (Interlocked.Increment(ref groupFetched) >= totalMessages) {
                                                        cancellation.Cancel();
                                                        break;
                                                    }
                                                }
                                                router.Log.Info(() => LogEvent.Create($"Member {member.member_id} finished batch size {batch.Messages.Count} ({groupFetched} of {totalMessages})"));
                                            }, merged.Token, batchSize);
                                        });
                                    }
                                }, timed.Token));
                            }
                        }
                        await Task.WhenAll(tasks);
                        Assert.That(allFetched, Is.AtLeast(totalMessages * groups));
    //                    await Task.WhenAny(Task.WhenAll(tasks), Task.Delay(TimeSpan.FromMinutes(1)));
                    }, 10);
                }
            }
        }

        #region helpers

        private void CheckMessages(List<Message> expected, IMessageBatch actual)
        {
            Assert.AreEqual(expected.Count(), actual.Messages.Count(), "Didn't get all messages");

            foreach (var message in expected)
            {
                Assert.IsTrue(actual.Messages.Any(m => m.Value.SequenceEqual(message.Value)), "Didn't get the same messages");
            }
        }

        private List<Message> CreateTestMessages(int count, int messageSize)
        {
            var messages = new List<Message>();

            for (var i = 0; i < count; i++) {
                var value = new byte[messageSize];
                for (var j = 0; j < messageSize; j++) {
                    value[j] = 1;
                }

                messages.Add(new Message(new ArraySegment<byte>(value), new ArraySegment<byte>(new byte[0]), 0));
            }

            return messages;
        }

        #endregion

        // design unit TESTS to write:
        // dealing correctly with losing ownership
        // can read messages from assigned partition(s)
        // multiple partition assignment test
        // multiple member test
        // assignment priority is given to first assignor if multiple available
    }
}