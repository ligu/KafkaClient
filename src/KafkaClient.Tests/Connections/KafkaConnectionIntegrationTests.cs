using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Connections;
using KafkaClient.Protocol;
using KafkaClient.Tests.Helpers;
using NUnit.Framework;

namespace KafkaClient.Tests.Connections
{
    /// <summary>
    /// Note these integration tests require an actively running kafka server defined in the app.config file.
    /// </summary>
    [TestFixture]
    [Category("Integration")]
    public class KafkaConnectionIntegrationTests
    {
        private Connection _conn;

        [SetUp]
        public void Setup()
        {
            var options = new KafkaOptions(IntegrationConfig.IntegrationUri, new ConnectionConfiguration(versionSupport: VersionSupport.Kafka8.MakeDynamic()), log: new ConsoleLog());
            var endpoint = new ConnectionFactory().Resolve(options.ServerUris.First(), options.Log);

            _conn = new Connection(new TcpSocket(endpoint, options.ConnectionConfiguration), options.ConnectionConfiguration, IntegrationConfig.NoDebugLog);
        }

        [Test]
        public async Task EnsureTwoRequestsCanCallOneAfterAnother()
        {
            var result1 = await _conn.SendAsync(new MetadataRequest(), CancellationToken.None);
            var result2 = await _conn.SendAsync(new MetadataRequest(), CancellationToken.None);
            Assert.That(result1.Errors.Count(code => code != ErrorResponseCode.None), Is.EqualTo(0));
            Assert.That(result2.Errors.Count(code => code != ErrorResponseCode.None), Is.EqualTo(0));
        }

        [Test]
        public async Task EnsureAsyncRequestResponsesCorrelate()
        {
            var result1 = _conn.SendAsync(new MetadataRequest(), CancellationToken.None);
            var result2 = _conn.SendAsync(new MetadataRequest(), CancellationToken.None);
            var result3 = _conn.SendAsync(new MetadataRequest(), CancellationToken.None);

            await Task.WhenAll(result1, result2, result3);

            Assert.That(result1.Result.Errors.Count(code => code != ErrorResponseCode.None), Is.EqualTo(0));
            Assert.That(result2.Result.Errors.Count(code => code != ErrorResponseCode.None), Is.EqualTo(0));
            Assert.That(result3.Result.Errors.Count(code => code != ErrorResponseCode.None), Is.EqualTo(0));
        }

        [Test]
        public async Task EnsureMultipleAsyncRequestsCanReadResponses([Values(1, 5)] int senders, [Values(10, 50, 200)] int totalRequests)
        {
            var requestsSoFar = 0;
            var requestTasks = new ConcurrentBag<Task<MetadataResponse>>();
            var singleResult = await _conn.SendAsync(new MetadataRequest(IntegrationConfig.TopicName()), CancellationToken.None);
            Assert.That(singleResult.Topics.Count, Is.GreaterThan(0));
            Assert.That(singleResult.Topics.First().Partitions.Count, Is.GreaterThan(0));

            var senderTasks = new List<Task>();
            for (var s = 0; s < senders; s++) {
                senderTasks.Add(Task.Run(async () => {
                    while (true) {
                        await Task.Delay(1);
                        if (Interlocked.Increment(ref requestsSoFar) > totalRequests) break;
                        requestTasks.Add(_conn.SendAsync(new MetadataRequest(), CancellationToken.None));
                    }
                }));
            }

            await Task.WhenAll(senderTasks);
            var requests = requestTasks.ToArray();
            await Task.WhenAll(requests);

            var results = requests.Select(x => x.Result).ToList();
            Assert.That(results.Count, Is.EqualTo(totalRequests));
        }

        [Test]
        public async Task EnsureDifferentTypesOfResponsesCanBeReadAsync()
        {
            //just ensure the topic exists for this test
            var ensureTopic = await _conn.SendAsync(new MetadataRequest(IntegrationConfig.TopicName()), CancellationToken.None);

            Assert.That(ensureTopic.Topics.Count, Is.EqualTo(1));
            Assert.That(ensureTopic.Topics.First().TopicName == IntegrationConfig.TopicName(), Is.True, "ProduceRequest did not return expected topic.");

            var result1 = _conn.SendAsync(RequestFactory.CreateProduceRequest(IntegrationConfig.TopicName(), "test"), CancellationToken.None);
            var result2 = _conn.SendAsync(new MetadataRequest(IntegrationConfig.TopicName()), CancellationToken.None);
            var result3 = _conn.SendAsync(RequestFactory.CreateOffsetRequest(IntegrationConfig.TopicName()), CancellationToken.None);
            var result4 = _conn.SendAsync(RequestFactory.CreateFetchRequest(IntegrationConfig.TopicName(), 0), CancellationToken.None);

            await Task.WhenAll(result1, result2, result3, result4);

            Assert.That(result1.Result.Topics.Count, Is.EqualTo(1));
            Assert.That(result1.Result.Topics.First().TopicName == IntegrationConfig.TopicName(), Is.True, "ProduceRequest did not return expected topic.");

            Assert.That(result2.Result.Topics.Count, Is.GreaterThan(0));
            Assert.That(result2.Result.Topics.Any(x => x.TopicName == IntegrationConfig.TopicName()), Is.True, "MetadataRequest did not return expected topic.");

            Assert.That(result3.Result.Topics.Count, Is.EqualTo(1));
            Assert.That(result3.Result.Topics.First().TopicName == IntegrationConfig.TopicName(), Is.True, "OffsetRequest did not return expected topic.");

            Assert.That(result4.Result.Topics.Count, Is.EqualTo(1));
            Assert.That(result4.Result.Topics.First().TopicName == IntegrationConfig.TopicName(), Is.True, "FetchRequest did not return expected topic.");
        }
    }
}