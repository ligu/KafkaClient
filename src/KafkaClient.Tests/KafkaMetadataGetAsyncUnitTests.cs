using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Connections;
using KafkaClient.Protocol;
using KafkaClient.Tests.Helpers;
using NUnit.Framework;

namespace KafkaClient.Tests
{
    [TestFixture]
    [Category("Integration")]
    public class KafkaMetadataGetAsyncUnitTests
    {
        private readonly KafkaOptions _options = new KafkaOptions(IntegrationConfig.IntegrationUri);

        private Connection GetKafkaConnection()
        {
            var endpoint = new ConnectionFactory().Resolve(_options.ServerUris.First(), _options.Log);
            var config = _options.ConnectionConfiguration;
            return new Connection(new TcpSocket(endpoint, config), config, _options.Log);
        }

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        [Ignore("Disable auto topic create in our server")]
        public async Task NewlyCreatedTopicShouldRetryUntilBrokerIsAssigned()
        {
            var expectedTopic = Guid.NewGuid().ToString();
            var router = new BrokerRouter(_options);
            var response = router.GetMetadataAsync(new []{ expectedTopic }, CancellationToken.None);
            var topic = (await response).Topics.FirstOrDefault();

            Assert.That(topic, Is.Not.Null);
            Assert.That(topic.TopicName, Is.EqualTo(expectedTopic));
            Assert.That(topic.ErrorCode, Is.EqualTo((int)ErrorResponseCode.None));
        }
    }
}