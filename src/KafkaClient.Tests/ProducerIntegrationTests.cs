using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Protocol;
using KafkaClient.Tests.Helpers;
using NUnit.Framework;

namespace KafkaClient.Tests
{
    [TestFixture]
    [Category("Integration")]
    public class ProducerIntegrationTests
    {
        [Test]
        public async Task ProducerShouldNotExpectResponseWhenAckIsZero()
        {
            using (var router = new Router(new KafkaOptions(TestConfig.IntegrationUri))) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        var sendTask = producer.SendMessageAsync(
                            new Message(Guid.NewGuid().ToString()), TestConfig.TopicName(), null,
                            new SendMessageConfiguration(acks: 0), CancellationToken.None);

                        await Task.WhenAny(sendTask, Task.Delay(TimeSpan.FromMinutes(2)));

                        Assert.That(sendTask.Status, Is.EqualTo(TaskStatus.RanToCompletion));
                    }
                });
            }
        }

        [Test]
        public async Task SendAsyncShouldGetOneResultForMessage()
        {
            using (var router = new Router(new KafkaOptions(TestConfig.IntegrationUri))) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        var result = await producer.SendMessagesAsync(new[] { new Message(Guid.NewGuid().ToString()) }, TestConfig.TopicName(), CancellationToken.None);

                        Assert.That(result.Count, Is.EqualTo(1));
                    }
                });
            }
        }

        [Test]
        public async Task SendAsyncShouldGetAResultForEachPartitionSentTo()
        {
            using (var router = new Router(new KafkaOptions(TestConfig.IntegrationUri))) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        var messages = new[] { new Message("1"), new Message("2"), new Message("3") };
                        var result = await producer.SendMessagesAsync(messages, TestConfig.TopicName(), CancellationToken.None);

                        Assert.That(result.Count, Is.EqualTo(messages.Distinct().Count()));

                        Assert.That(result.Count, Is.EqualTo(messages.Count()));
                    }
                });
            }
        }

        [Test]
        public async Task SendAsyncShouldGetOneResultForEachPartitionThroughBatching()
        {
            using (var router = new Router(new KafkaOptions(TestConfig.IntegrationUri))) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        var tasks = new[] {
                            producer.SendMessageAsync(new Message("1"), TestConfig.TopicName(), CancellationToken.None),
                            producer.SendMessageAsync(new Message("2"), TestConfig.TopicName(), CancellationToken.None),
                            producer.SendMessageAsync(new Message("3"), TestConfig.TopicName(), CancellationToken.None),
                        };

                        await Task.WhenAll(tasks);

                        var result = tasks.Select(x => x.Result).Distinct().ToList();
                        Assert.That(result.Count, Is.EqualTo(tasks.Length));
                    }
                });
            }
        }
    }
}