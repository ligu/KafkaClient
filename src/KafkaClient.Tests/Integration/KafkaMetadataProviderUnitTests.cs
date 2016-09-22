using System;
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
    public class KafkaMetadataProviderUnitTests
    {
        private readonly KafkaOptions _options = new KafkaOptions(IntegrationConfig.IntegrationUri);

        private KafkaConnection GetKafkaConnection()
        {
            var endpoint = new KafkaConnectionFactory().Resolve(_options.ServerUris.First(), _options.Log);
            return new KafkaConnection(new KafkaTcpSocket(new TraceLog(), endpoint, 5), _options.ConnectionOptions.RequestTimeout, _options.Log);
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [Ignore("Disable auto topic create in our server")]

        public async Task NewlyCreatedTopicShouldRetryUntilBrokerIsAssigned()
        {
            var expectedTopic = Guid.NewGuid().ToString();
            var repo = new KafkaMetadataProvider(_options.Log);
            var response = repo.GetAsync(new[] { GetKafkaConnection() }, new[] { expectedTopic }, CancellationToken.None);
            var topic = (await response).Topics.FirstOrDefault();

            Assert.That(topic, Is.Not.Null);
            Assert.That(topic.TopicName, Is.EqualTo(expectedTopic));
            Assert.That(topic.ErrorCode, Is.EqualTo((int)ErrorResponseCode.NoError));
        }
    }
}