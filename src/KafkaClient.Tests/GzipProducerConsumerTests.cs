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

namespace KafkaClient.Tests
{
    [TestFixture]
    [Category("Integration")]
    public class GzipProducerConsumerTests
    {
        private readonly KafkaOptions _options = new KafkaOptions(IntegrationConfig.IntegrationUri, log: new ConsoleLog(LogLevel.Info));

        private Connection GetKafkaConnection()
        {
            var endpoint = new ConnectionFactory().Resolve(_options.ServerUris.First(), _options.Log);
            var configuration = _options.ConnectionConfiguration;
            return new Connection(new TcpSocket(endpoint, configuration), configuration, _options.Log);
        }

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public async Task EnsureGzipCompressedMessageCanSend()
        {
            var topicName = IntegrationConfig.TopicName();
            IntegrationConfig.NoDebugLog.Info(() => LogEvent.Create(">> Start EnsureGzipCompressedMessageCanSend"));
            using (var conn = GetKafkaConnection()) {
                await conn.SendAsync(new MetadataRequest(topicName), CancellationToken.None);
            }

            using (var router = new BrokerRouter(_options)) {
                IntegrationConfig.NoDebugLog.Info(() => LogEvent.Create(">> Start GetTopicMetadataAsync"));
                await router.GetTopicMetadataAsync(topicName, CancellationToken.None);
                IntegrationConfig.NoDebugLog.Info(() => LogEvent.Create(">> End GetTopicMetadataAsync"));
                var conn = router.GetBrokerRoute(topicName, 0);

                var request = new ProduceRequest(new ProduceRequest.Payload(topicName, 0, new [] {
                                    new Message("0", "1"),
                                    new Message("1", "1"),
                                    new Message("2", "1")
                                }, MessageCodec.CodecGzip));
                IntegrationConfig.NoDebugLog.Info(() => LogEvent.Create(">> start SendAsync"));
                var response = await conn.Connection.SendAsync(request, CancellationToken.None);
                IntegrationConfig.NoDebugLog.Info(() => LogEvent.Create("end SendAsync"));
                Assert.That(response.Errors.Any(e => e != ErrorResponseCode.None), Is.False);
                IntegrationConfig.NoDebugLog.Info(() => LogEvent.Create("start dispose"));
            }
            IntegrationConfig.NoDebugLog.Info(() => LogEvent.Create(">> End EnsureGzipCompressedMessageCanSend"));
        }

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public async Task EnsureGzipCanDecompressMessageFromKafka()
        {
            var numberOfMessages = 3;
            var topicName = IntegrationConfig.TopicName();
            var partitionId = 0;

            IntegrationConfig.NoDebugLog.Info(() => LogEvent.Create(">> Start EnsureGzipCanDecompressMessageFromKafka"));
            using (var router = new BrokerRouter(_options)) {
                using (var producer = new Producer(router, new ProducerConfiguration(batchSize: numberOfMessages)))
                {
                    var offset = await producer.BrokerRouter.GetTopicOffsetAsync(topicName, 0, CancellationToken.None);
                    var consumer = new Consumer(router);
                    var messages = new List<Message>();
                    for (var i = 0; i < numberOfMessages; i++) {
                        messages.Add(new Message(i.ToString()));
                    }
                    IntegrationConfig.NoDebugLog.Info(() => LogEvent.Create(">> Start SendMessagesAsync"));
                    await producer.SendMessagesAsync(messages, topicName, partitionId, new SendMessageConfiguration(codec: MessageCodec.CodecGzip), CancellationToken.None);
                    IntegrationConfig.NoDebugLog.Info(() => LogEvent.Create(">> End SendMessagesAsync"));

                    IntegrationConfig.NoDebugLog.Info(() => LogEvent.Create(">> Start Consume"));
                    var results = await consumer.FetchMessagesAsync(offset, messages.Count, CancellationToken.None);
                    IntegrationConfig.NoDebugLog.Info(() => LogEvent.Create(">> End Consume"));
                    Assert.That(results, Is.Not.Null);
                    Assert.That(results.Count, Is.EqualTo(messages.Count));
                    for (var i = 0; i < messages.Count; i++) {
                        Assert.That(results[i].Value.ToUtf8String(), Is.EqualTo(i.ToString()));
                    }
                }
            }
            IntegrationConfig.NoDebugLog.Info(() => LogEvent.Create(">> End EnsureGzipCanDecompressMessageFromKafka"));
        }
    }
}