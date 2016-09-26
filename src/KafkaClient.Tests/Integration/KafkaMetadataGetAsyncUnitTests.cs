using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Connection;
using KafkaClient.Protocol;
using KafkaClient.Tests.Helpers;
using NUnit.Framework;

namespace KafkaClient.Tests.Integration
{
    [TestFixture]
    [Category("Integration")]
    public class KafkaMetadataGetAsyncUnitTests
    {
        private readonly KafkaOptions _options = new KafkaOptions(IntegrationConfig.IntegrationUri);

        private Connection.Connection GetKafkaConnection()
        {
            var endpoint = new ConnectionFactory().Resolve(_options.ServerUris.First(), _options.Log);
            var config = _options.ConnectionConfiguration;
            return new Connection.Connection(new TcpSocket(endpoint, config), config, _options.Log);
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [Ignore("Disable auto topic create in our server")]
        public async Task NewlyCreatedTopicShouldRetryUntilBrokerIsAssigned()
        {
            var expectedTopic = Guid.NewGuid().ToString();
            var response = new MetadataRequest(expectedTopic).GetAsync(new[] { GetKafkaConnection() }, _options.Log, CancellationToken.None);
            var topic = (await response).Topics.FirstOrDefault();

            Assert.That(topic, Is.Not.Null);
            Assert.That(topic.TopicName, Is.EqualTo(expectedTopic));
            Assert.That(topic.ErrorCode, Is.EqualTo((int)ErrorResponseCode.NoError));
        }
    }
}