using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Protocol;
using KafkaClient.Tests.Helpers;
using NSubstitute;
using NUnit.Framework;

namespace KafkaClient.Tests
{
    [TestFixture]
    [Category("Integration")]
    public class ProducerConsumerTests
    {
        [Test]
        public async Task ProducerAckLevel()
        {
            using (var router = new Router(TestConfig.IntegrationUri, log: TestConfig.InfoLog)) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        var responseAckLevel0 = await producer.SendMessageAsync(new Message("Ack Level 0"), topicName, 0, new SendMessageConfiguration(acks: 0), CancellationToken.None);
                        Assert.AreEqual(responseAckLevel0.Offset, -1);
                        var responseAckLevel1 = await producer.SendMessageAsync(new Message("Ack Level 1"), topicName, 0, new SendMessageConfiguration(acks: 1), CancellationToken.None);
                        Assert.That(responseAckLevel1.Offset, Is.GreaterThan(-1));
                    }
                });
            }
        }

        [Test]
        public async Task ProducerAckLevel1ResponseOffsetShouldBeEqualToLastOffset()
        {
            using (var router = new Router(TestConfig.IntegrationUri, log: TestConfig.InfoLog )) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        var responseAckLevel1 = await producer.SendMessageAsync(new Message("Ack Level 1"), topicName, 0, new SendMessageConfiguration(acks: 1), CancellationToken.None);
                        var offsetResponse = await producer.Router.GetTopicOffsetsAsync(topicName, CancellationToken.None);
                        var maxOffset = offsetResponse.First(x => x.PartitionId == 0);
                        Assert.AreEqual(responseAckLevel1.Offset, maxOffset.Offset - 1);
                    }
                });
            }
        }

        [Test]
        public async Task ProducerLastResposeOffsetAckLevel1ShouldBeEqualsToLastOffset()
        {
            using (var router = new Router(TestConfig.IntegrationUri, log: TestConfig.InfoLog)) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        var responseAckLevel1 =
                            await producer.SendMessagesAsync(
                                new[] { new Message("Ack Level 1"), new Message("Ack Level 1") }, topicName, 0,
                                new SendMessageConfiguration(acks: 1), CancellationToken.None);
                        var offsetResponse = await producer.Router.GetTopicOffsetsAsync(
                            topicName, CancellationToken.None);
                        var maxOffset = offsetResponse.First(x => x.PartitionId == 0);

                        Assert.AreEqual(responseAckLevel1.Last().Offset, maxOffset.Offset - 1);
                    }
                });
            }
        }

        [Test]
        public async Task ConsumeByOffsetShouldGetSameMessageProducedAtSameOffset()
        {
            long offsetResponse;
            var messge = Guid.NewGuid();

            using (var router = new Router(TestConfig.IntegrationUri, log: TestConfig.InfoLog)) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        var responseAckLevel1 = await producer.SendMessageAsync(new Message(messge.ToString()), topicName, 0, new SendMessageConfiguration(acks: 1), CancellationToken.None);
                        offsetResponse = responseAckLevel1.Offset;
                    }
                    using (var consumer = new Consumer(router, new ConsumerConfiguration(maxServerWait: TimeSpan.Zero))) {
                        var result = await consumer.FetchMessagesAsync(topicName, 0, offsetResponse, 1, CancellationToken.None);
                        Assert.AreEqual(messge.ToString(), result[0].Value.ToUtf8String());
                    }
                });
            }
        }

        [Test]
        public async Task ConsumerShouldConsumeInSameOrderAsProduced([Values(20)] int sends, [Values(1, 10)] int messagesPerSend)
        {
            var totalMessages = sends * messagesPerSend;

            var expected = totalMessages.Repeat(i => i.ToString()).ToList();
            var testId = Guid.NewGuid().ToString();

            using (var router = new Router(TestConfig.IntegrationUri, log: TestConfig.InfoLog )) {
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
                            var results = await consumer.FetchMessagesAsync(offset, totalMessages, CancellationToken.None);

                            //ensure the produced messages arrived
                            TestConfig.InfoLog.Info(() => LogEvent.Create($"Message order:  {string.Join(", ", results.Select(x => x.Value.ToUtf8String()).ToList())}"));

                            Assert.That(results.Count, Is.EqualTo(totalMessages));
                            Assert.That(results.Select(x => x.Value.ToUtf8String()).ToList(), Is.EqualTo(expected), "Expected the message list in the correct order.");
                            Assert.That(results.Any(x => x.Key.ToUtf8String() != testId), Is.False);                    
                        }
                    }
                });
            }
        }

        [Test]
        public async Task ConsumerShouldBeAbleToSeekBackToEarlierOffset([Values(20)] int sends, [Values(1, 10)] int messagesPerSend)
        {
            var totalMessages = sends * messagesPerSend;

            var testId = Guid.NewGuid().ToString();

            using (var router = new Router(TestConfig.IntegrationUri, log: TestConfig.InfoLog )) {
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
                            var results1 = await consumer.FetchMessagesAsync(offset, totalMessages, CancellationToken.None);
                            TestConfig.InfoLog.Info(() => LogEvent.Create($"Message order:  {string.Join(", ", results1.Select(x => x.Value.ToUtf8String()).ToList())}"));

                            var results2 = await consumer.FetchMessagesAsync(offset, totalMessages, CancellationToken.None);
                            TestConfig.InfoLog.Info(() => LogEvent.Create($"Message order:  {string.Join(", ", results2.Select(x => x.Value.ToUtf8String()).ToList())}"));

                            Assert.That(results1.Count, Is.EqualTo(totalMessages));
                            Assert.That(results1.Count, Is.EqualTo(results2.Count));
                            Assert.That(results1.Select(x => x.Value.ToUtf8String()).ToList(), Is.EqualTo(results2.Select(x => x.Value.ToUtf8String()).ToList()), "Expected the message list in the correct order.");
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

            using (var router = new Router(TestConfig.IntegrationUri, log: TestConfig.InfoLog )) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        var offset = await producer.Router.GetTopicOffsetAsync(topicName, 0, CancellationToken.None);

                        for (var i = 0; i < totalMessages; i++) {
                            await producer.SendMessageAsync(new Message(i.ToString(), testId), topicName, 0, CancellationToken.None);
                        }

                        using (var consumer = new Consumer(router, new ConsumerConfiguration(maxServerWait: TimeSpan.Zero))) {
                            var results = await consumer.FetchMessagesAsync(offset, totalMessages, CancellationToken.None);
                            TestConfig.InfoLog.Info(() => LogEvent.Create($"Message order:  {string.Join(", ", results.Select(x => x.Value.ToUtf8String()).ToList())}"));

                            Assert.That(results.Count, Is.EqualTo(totalMessages));
                            Assert.That(results.Select(x => x.Value.ToUtf8String()).ToList(), Is.EqualTo(expected), "Expected the message list in the correct order.");

                            var newOffset = await producer.Router.GetTopicOffsetAsync(offset.TopicName, offset.PartitionId, CancellationToken.None);
                            Assert.That(newOffset.Offset - offset.Offset, Is.EqualTo(totalMessages));
                        }
                    }
                });
            }
        }

        [Test]
        public async Task ProducerShouldUsePartitionIdInsteadOfMessageKeyToChoosePartition()
        {
            var partitionSelector = Substitute.For<IPartitionSelector>();
            partitionSelector.Select(null, null)
                             .ReturnsForAnyArgs(_ => _.Arg<MetadataResponse.Topic>().Partitions.Single(p => p.PartitionId == 1));

            using (var router = new Router(new KafkaOptions(TestConfig.IntegrationUri, partitionSelector: partitionSelector))) {
                await router.TemporaryTopicAsync(async topicName => {
                var offset = await router.GetTopicOffsetAsync(topicName, 0, CancellationToken.None);
                    using (var producer = new Producer(router)) {
                        //message should send to PartitionId and not use the key to Select Broker Route !!
                        for (var i = 0; i < 20; i++) {
                            await producer.SendMessageAsync(new Message(i.ToString(), "key"), offset.TopicName, offset.PartitionId, CancellationToken.None);
                        }
                    }

                    using (var consumer = new Consumer(router)) {
                        using (var source = new CancellationTokenSource()) {
                            var i = 0;
                            await consumer.FetchAsync(
                                offset, 20, message =>
                                {
                                    Assert.That(message.Value.ToUtf8String(), Is.EqualTo(i++.ToString()));
                                    if (i >= 20) {
                                        source.Cancel();
                                    }
                                    return Task.FromResult(0);
                                }, source.Token);
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

        [Test]
        public async Task ConsumerShouldMoveToNextAvailableOffsetWhenQueryingForNextMessage()
        {
            const int expectedCount = 1000;
            var options = new KafkaOptions(TestConfig.IntegrationUri);

            using (var router = new Router(options)) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        //get current offset and reset consumer to top of log
                        var offsets = await producer.Router.GetTopicOffsetsAsync(topicName, CancellationToken.None).ConfigureAwait(false);

                        using (var consumerRouter = new Router(options))
                        using (var consumer = new OldConsumer(new ConsumerOptions(topicName, consumerRouter) { MaxWaitTimeForMinimumBytes = TimeSpan.Zero },
                             offsets.Select(x => new OffsetPosition(x.PartitionId, x.Offset)).ToArray()))
                        {
                            Console.WriteLine("Sending {0} test messages", expectedCount);
                            var response = await producer.SendMessagesAsync(Enumerable.Range(0, expectedCount).Select(x => new Message(x.ToString())), topicName, CancellationToken.None);

                            Assert.That(response.Any(x => x.ErrorCode != (int)ErrorResponseCode.None), Is.False, "Error occured sending test messages to server.");

                            var stream = consumer.Consume();

                            Console.WriteLine("Reading message back out from consumer.");
                            var data = stream.Take(expectedCount).ToList();

                            var consumerOffset = consumer.GetOffsetPosition().OrderBy(x => x.PartitionId).ToList();

                            var serverOffset = await producer.Router.GetTopicOffsetsAsync(topicName, CancellationToken.None).ConfigureAwait(false);
                            var positionOffset = serverOffset.Select(x => new OffsetPosition(x.PartitionId, x.Offset))
                                .OrderBy(x => x.PartitionId)
                                .ToList();

                            Assert.That(consumerOffset, Is.EqualTo(positionOffset), "The consumerOffset position should match the server offset position.");
                            Assert.That(data.Count, Is.EqualTo(expectedCount), "We should have received 2000 messages from the server.");
                        }
                    }
                });
            }
        }
    }
}