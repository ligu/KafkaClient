using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Protocol;
using KafkaClient.Tests.Helpers;
using NUnit.Framework;

namespace KafkaClient.Tests.Integration
{
    [TestFixture]
    [Category("Integration")]
    public class ProducerIntegrationTests
    {
        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void ProducerShouldNotExpectResponseWhenAckIsZero()
        {
            using (var router = new BrokerRouter(new KafkaOptions(IntegrationConfig.IntegrationUri)))
            using (var producer = new Producer(router))
            {
                var sendTask = producer.SendMessageAsync(new Message(Guid.NewGuid().ToString()), IntegrationConfig.IntegrationTopic, null, new SendMessageConfiguration(acks: 0), CancellationToken.None);

                sendTask.Wait(TimeSpan.FromMinutes(2));

                Assert.That(sendTask.Status, Is.EqualTo(TaskStatus.RanToCompletion));
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async void SendAsyncShouldGetOneResultForMessage()
        {
            using (var router = new BrokerRouter(new KafkaOptions(IntegrationConfig.IntegrationUri)))
            using (var producer = new Producer(router))
            {
                var result = await producer.SendMessagesAsync(new[] { new Message(Guid.NewGuid().ToString()) }, IntegrationConfig.IntegrationTopic, CancellationToken.None);

                Assert.That(result.Length, Is.EqualTo(1));
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async void SendAsyncShouldGetAResultForEachPartitionSentTo()
        {
            using (var router = new BrokerRouter(new KafkaOptions(IntegrationConfig.IntegrationUri)))
            using (var producer = new Producer(router))
            {
                var messages = new[] { new Message("1"), new Message("2"), new Message("3") };
                var result = await producer.SendMessagesAsync(messages, IntegrationConfig.IntegrationTopic, CancellationToken.None);

                Assert.That(result.Length, Is.EqualTo(messages.Distinct().Count()));

                Assert.That(result.Length, Is.EqualTo(messages.Count()));
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async void SendAsyncShouldGetOneResultForEachPartitionThroughBatching()
        {
            using (var router = new BrokerRouter(new KafkaOptions(IntegrationConfig.IntegrationUri)))
            using (var producer = new Producer(router))
            {
                var tasks = new[] {
                    producer.SendMessageAsync(new Message("1"), IntegrationConfig.IntegrationTopic, CancellationToken.None),
                    producer.SendMessageAsync(new Message("2"), IntegrationConfig.IntegrationTopic, CancellationToken.None),
                    producer.SendMessageAsync(new Message("3"), IntegrationConfig.IntegrationTopic, CancellationToken.None),
                };

                await Task.WhenAll(tasks);

                var result = tasks.Select(x => x.Result).Distinct().ToList();
                Assert.That(result.Count, Is.EqualTo(tasks.Length));
            }
        }
    }
}