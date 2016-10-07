using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Protocol;
using NUnit.Framework;

namespace KafkaClient.Tests
{
    [TestFixture]
    [Category("Integration")]
    internal class ManualTesting
    {
        private readonly KafkaOptions _options = new KafkaOptions(new []{ new Uri("http://S1.com:9092"), new Uri("http://S2.com:9092"), new Uri("http://S3.com:9092") }, log: new TraceLog(LogLevel.Warn));
        public readonly  TraceLog _log = new TraceLog(LogLevel.Debug);

        /// <summary>
        /// These tests are for manual run. You need to stop the partition leader and then start it again and let it became the leader.        
        /// </summary>

        [Test]
        [Ignore("manual test")]
        public void ConsumerFailure()
        {
            string topic = "TestTopicIssue13-2-3R-1P";
            using (var router = new BrokerRouter(_options))
            {
                var producer = new Producer(router);
                var offsets = producer.BrokerRouter.GetTopicOffsetAsync(topic, CancellationToken.None).Result;
                var maxOffsets = offsets.Select(x => new OffsetPosition(x.PartitionId, x.Offsets.Max())).ToArray();
                var consumerOptions = new ConsumerOptions(topic, router) { PartitionWhitelist = new List<int>() { 0 }, MaxWaitTimeForMinimumBytes = TimeSpan.Zero };

                SandMessageForever(producer, topic);
                ReadMessageForever(consumerOptions, maxOffsets);
            }
        }

        [Test]
        [Ignore("manual test")]
        public async Task ManualConsumerFailure()
        {
            string topic = "TestTopicIssue13-3R-1P";
            var manualConsumer = new ManualConsumer(0, topic, new BrokerRouter(_options), "test client", 10000);
            long offset = await manualConsumer.FetchLastOffsetAsync(CancellationToken.None);

            var router = new BrokerRouter(_options);
            var producer = new Producer(router);
            SandMessageForever(producer, topic);
            await ReadMessageForever(manualConsumer, offset);
        }

   

        private  void ReadMessageForever(ConsumerOptions consumerOptions, OffsetPosition[] maxOffsets)
        {
            using (var consumer = new Consumer(consumerOptions, maxOffsets))
            {
                var blockingEnumerableOfMessage = consumer.Consume();
                foreach (var message in blockingEnumerableOfMessage)
                {
                    _log.Info(() => LogEvent.Create($"Offset{message.Offset}"));
                }
            }
        }

        private  void SandMessageForever(Producer producer, string topic)
        {
            var sandMessageForever = Task.Run(() =>
            {
                int id = 0;
                while (true)
                {
                    try
                    {
                        producer.SendMessageAsync(new Message((++id).ToString()), topic, 0, CancellationToken.None).Wait();
                        Thread.Sleep(1000);
                    }
                    catch (Exception ex)
                    {
                        _log.Info(() => LogEvent.Create(ex, "can't send:"));
                    }
                }
            });
        }

        private  async Task ReadMessageForever(ManualConsumer manualConsumer, long offset)
        {
            while (true)
            {
                try
                {
                    var messages = await manualConsumer.FetchMessagesAsync(1000, offset, CancellationToken.None);

                    if (messages.Any())
                    {
                        foreach (var message in messages)
                        {
                            _log.Info(() => LogEvent.Create($"Offset{message.Offset}"));
                        }
                        offset = messages.Max(x => x.Offset) + 1;
                    }
                    else
                    {
                        await Task.Delay(100);
                    }
                }
                catch (Exception ex)
                {
                    _log.Info(() => LogEvent.Create(ex, "can't read:"));
                }
            }
        }
    }
}