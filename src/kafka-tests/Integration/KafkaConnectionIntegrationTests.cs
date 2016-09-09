using kafka_tests.Helpers;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using NUnit.Framework;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace kafka_tests.Integration
{
    /// <summary>
    /// Note these integration tests require an actively running kafka server defined in the app.config file.
    /// </summary>
    [TestFixture]
    [Category("Integration")]
    public class KafkaConnectionIntegrationTests
    {
        private KafkaConnection _conn;

        [SetUp]
        public void Setup()
        {
            var options = new KafkaOptions(IntegrationConfig.IntegrationUri);
            var endpoint = new DefaultKafkaConnectionFactory().Resolve(options.KafkaServerUri.First(), options.Log);

            _conn = new KafkaConnection(new KafkaTcpSocket(new DefaultTraceLog(), endpoint, 5), options.ResponseTimeoutMs, options.Log);
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void EnsureTwoRequestsCanCallOneAfterAnother()
        {
            var result1 = _conn.SendAsync(new MetadataRequest()).Result;
            var result2 = _conn.SendAsync(new MetadataRequest()).Result;
            Assert.That(result1.Errors.Count(code => code != ErrorResponseCode.NoError), Is.EqualTo(0));
            Assert.That(result2.Errors.Count(code => code != ErrorResponseCode.NoError), Is.EqualTo(0));
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void EnsureAsyncRequestResponsesCorrelate()
        {
            var result1 = _conn.SendAsync(new MetadataRequest());
            var result2 = _conn.SendAsync(new MetadataRequest());
            var result3 = _conn.SendAsync(new MetadataRequest());

            Assert.That(result1.Result.Errors.Count(code => code != ErrorResponseCode.NoError), Is.EqualTo(0));
            Assert.That(result2.Result.Errors.Count(code => code != ErrorResponseCode.NoError), Is.EqualTo(0));
            Assert.That(result3.Result.Errors.Count(code => code != ErrorResponseCode.NoError), Is.EqualTo(0));
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void EnsureMultipleAsyncRequestsCanReadResponses()
        {
            var requests = new List<Task<MetadataResponse>>();
            var singleResult = _conn.SendAsync(new MetadataRequest(IntegrationConfig.IntegrationTopic)).Result;
            Assert.That(singleResult.Topics.Count, Is.GreaterThan(0));
            Assert.That(singleResult.Topics.First().Partitions.Count, Is.GreaterThan(0));

            for (int i = 0; i < 20; i++)
            {
                requests.Add(_conn.SendAsync(new MetadataRequest()));
            }

            var results = requests.Select(x => x.Result).ToList();
            Assert.That(results.Count, Is.EqualTo(20));
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void EnsureDifferentTypesOfResponsesCanBeReadAsync()
        {
            //just ensure the topic exists for this test
            var ensureTopic = _conn.SendAsync(new MetadataRequest(IntegrationConfig.IntegrationTopic)).Result;

            Assert.That(ensureTopic.Topics.Count, Is.EqualTo(1));
            Assert.That(ensureTopic.Topics.First().TopicName == IntegrationConfig.IntegrationTopic, Is.True, "ProduceRequest did not return expected topic.");

            var result1 = _conn.SendAsync(RequestFactory.CreateProduceRequest(IntegrationConfig.IntegrationTopic, "test"));
            var result2 = _conn.SendAsync(new MetadataRequest());
            var result3 = _conn.SendAsync(RequestFactory.CreateOffsetRequest(IntegrationConfig.IntegrationTopic));
            var result4 = _conn.SendAsync(RequestFactory.CreateFetchRequest(IntegrationConfig.IntegrationTopic, 0));

            Assert.That(result1.Result.Topics.Count, Is.EqualTo(1));
            Assert.That(result1.Result.Topics.First().TopicName == IntegrationConfig.IntegrationTopic, Is.True, "ProduceRequest did not return expected topic.");

            Assert.That(result2.Result.Topics.Count, Is.GreaterThan(0));
            Assert.That(result2.Result.Topics.Any(x => x.TopicName == IntegrationConfig.IntegrationTopic), Is.True, "MetadataRequest did not return expected topic.");

            Assert.That(result3.Result.Topics.Count, Is.EqualTo(1));
            Assert.That(result3.Result.Topics.First().TopicName == IntegrationConfig.IntegrationTopic, Is.True, "OffsetRequest did not return expected topic.");

            Assert.That(result4.Result.Topics.Count, Is.EqualTo(1));
            Assert.That(result4.Result.Topics.First().TopicName == IntegrationConfig.IntegrationTopic, Is.True, "FetchRequest did not return expected topic.");
        }
    }
}