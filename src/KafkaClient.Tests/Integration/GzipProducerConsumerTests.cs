using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Connection;
using KafkaClient.Protocol;
using KafkaClient.Tests.Helpers;
using NUnit.Framework;

namespace KafkaClient.Tests.Integration
{
    [TestFixture]
    [Category("Integration")]
    public class GzipProducerConsumerTests
    {
        private readonly KafkaOptions _options = new KafkaOptions(IntegrationConfig.IntegrationUri);

        private Connection.Connection GetKafkaConnection()
        {
            var endpoint = new ConnectionFactory().Resolve(_options.ServerUris.First(), _options.Log);
            var configuration = _options.ConnectionConfiguration;
            return new Connection.Connection(new TcpSocket(endpoint, configuration), configuration, _options.Log);
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [Ignore]
        public async Task EnsureGzipCompressedMessageCanSend()
        {
            IntegrationConfig.NoDebugLog.Info(() => LogEvent.Create(IntegrationConfig.Highlight("start EnsureGzipCompressedMessageCanSend")));
            using (var conn = GetKafkaConnection())
            {
                conn.SendAsync(new MetadataRequest(IntegrationConfig.IntegrationCompressionTopic), CancellationToken.None)
                    .Wait(TimeSpan.FromSeconds(10));
            }

            using (var router = new BrokerRouter(_options))
            {
                IntegrationConfig.NoDebugLog.Info(() => LogEvent.Create(IntegrationConfig.Highlight("start RefreshMissingTopicMetadataAsync")));
                await router.GetTopicMetadataAsync(IntegrationConfig.IntegrationCompressionTopic, CancellationToken.None);
                IntegrationConfig.NoDebugLog.Info(() => LogEvent.Create(IntegrationConfig.Highlight("end RefreshMissingTopicMetadataAsync")));
                var conn = router.GetBrokerRoute(IntegrationConfig.IntegrationCompressionTopic, 0);

                var request = new ProduceRequest(new Payload(IntegrationConfig.IntegrationCompressionTopic, 0, new [] {
                                    new Message("0", "1"),
                                    new Message("1", "1"),
                                    new Message("2", "1")
                                }, MessageCodec.CodecGzip));
                IntegrationConfig.NoDebugLog.Info(() => LogEvent.Create(IntegrationConfig.Highlight("start SendAsync")));
                var response = conn.Connection.SendAsync(request, CancellationToken.None).Result;
                IntegrationConfig.NoDebugLog.Info(() => LogEvent.Create("end SendAsync"));
                Assert.That(response.Errors.Any(e => e != ErrorResponseCode.NoError), Is.False);
                IntegrationConfig.NoDebugLog.Info(() => LogEvent.Create("start dispose"));
            }
            IntegrationConfig.NoDebugLog.Info(() => LogEvent.Create(IntegrationConfig.Highlight("end EnsureGzipCompressedMessageCanSend")));
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task EnsureGzipCanDecompressMessageFromKafka([Values(3)]int numberOfMessages)
        {
            var router = new BrokerRouter(_options);
            using (var producer = new Producer(router, new ProducerConfiguration(batchSize: numberOfMessages))) {
                var offsets = await producer.BrokerRouter.GetTopicOffsetAsync(IntegrationConfig.IntegrationCompressionTopic, CancellationToken.None);
                var offsetPositions = offsets.Where(x => !x.Offsets.IsEmpty).Select(x => new OffsetPosition(x.PartitionId, x.Offsets.Max()));
                var consumerOptions = new ConsumerOptions(IntegrationConfig.IntegrationCompressionTopic, router) { PartitionWhitelist = new List<int> { 0 } };
                using (var consumer = new Consumer(consumerOptions, offsetPositions.ToArray()))
                {
                    var messages = new List<Message>();
                    for (var i = 0; i < numberOfMessages; i++) {
                        messages.Add(new Message(i.ToString()));
                    }
                    await producer.SendMessagesAsync(messages, IntegrationConfig.IntegrationCompressionTopic, 0, new SendMessageConfiguration(codec: MessageCodec.CodecGzip), CancellationToken.None);

                    var results = consumer.Consume(new CancellationTokenSource(TimeSpan.FromMinutes(1)).Token).Take(messages.Count).ToList();
                    for (var i = 0; i < messages.Count; i++)
                    {
                        Assert.That(results[i].Value.ToUtf8String(), Is.EqualTo(i.ToString()));
                    }
                }
            }
        }
    }
}