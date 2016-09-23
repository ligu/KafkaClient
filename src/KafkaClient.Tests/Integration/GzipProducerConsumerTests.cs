using System;
using System.Collections.Generic;
using System.Linq;
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
        private readonly KafkaOptions _options = new KafkaOptions(IntegrationConfig.IntegrationUri, log: IntegrationConfig.NoDebugLog);

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
            IntegrationConfig.NoDebugLog.InfoFormat(IntegrationConfig.Highlight("start EnsureGzipCompressedMessageCanSend"));
            using (var conn = GetKafkaConnection())
            {
                conn.SendAsync(new MetadataRequest(IntegrationConfig.IntegrationCompressionTopic), CancellationToken.None)
                    .Wait(TimeSpan.FromSeconds(10));
            }

            using (var router = new BrokerRouter(_options))
            {
                IntegrationConfig.NoDebugLog.InfoFormat(IntegrationConfig.Highlight("start RefreshMissingTopicMetadataAsync"));
                await router.GetTopicMetadataAsync(IntegrationConfig.IntegrationCompressionTopic, CancellationToken.None);
                IntegrationConfig.NoDebugLog.InfoFormat(IntegrationConfig.Highlight("end RefreshMissingTopicMetadataAsync"));
                var conn = router.GetBrokerRoute(IntegrationConfig.IntegrationCompressionTopic, 0);

                var request = new ProduceRequest(new Payload(IntegrationConfig.IntegrationCompressionTopic, 0, new [] {
                                    new Message("0", "1"),
                                    new Message("1", "1"),
                                    new Message("2", "1")
                                }, MessageCodec.CodecGzip));
                IntegrationConfig.NoDebugLog.InfoFormat(IntegrationConfig.Highlight("start SendAsync"));
                var response = conn.Connection.SendAsync(request, CancellationToken.None).Result;
                IntegrationConfig.NoDebugLog.InfoFormat("end SendAsync");
                Assert.That(response.Errors.Any(e => e != ErrorResponseCode.NoError), Is.False);
                IntegrationConfig.NoDebugLog.InfoFormat("start dispose");
            }
            IntegrationConfig.NoDebugLog.InfoFormat(IntegrationConfig.Highlight("end EnsureGzipCompressedMessageCanSend"));
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void EnsureGzipCanDecompressMessageFromKafka()
        {
            var router = new BrokerRouter(_options);
            var producer = new Producer(router);

            var offsets = producer.BrokerRouter.GetTopicOffsetAsync(IntegrationConfig.IntegrationCompressionTopic, CancellationToken.None).Result;

            var offsetPositions = offsets.Where(x => !x.Offsets.IsEmpty).Select(x => new OffsetPosition(x.PartitionId, x.Offsets.Max()));
            var consumer = new Consumer(new ConsumerOptions(IntegrationConfig.IntegrationCompressionTopic, router) {
                                            PartitionWhitelist = new List<int> {0}
                                        }, offsetPositions.ToArray());
            int numberOfmessage = 3;
            for (int i = 0; i < numberOfmessage; i++)
            {
                producer.SendMessageAsync(IntegrationConfig.IntegrationCompressionTopic, new[] { new Message(i.ToString()) }, codec: MessageCodec.CodecGzip,
              partition: 0);
            }

            var results = consumer.Consume(new CancellationTokenSource(TimeSpan.FromMinutes(1)).Token).Take(numberOfmessage).ToList();

            for (int i = 0; i < numberOfmessage; i++)
            {
                Assert.That(results[i].Value.ToUtf8String(), Is.EqualTo(i.ToString()));
            }

            using (producer)
            using (consumer) { }
        }
    }
}