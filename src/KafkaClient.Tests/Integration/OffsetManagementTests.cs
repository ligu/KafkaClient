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
    public class OffsetManagementTests
    {
        private readonly KafkaOptions Options = new KafkaOptions(IntegrationConfig.IntegrationUri);

        [SetUp]
        public void Setup()
        {
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [Ignore("Not supported currently in 8.1.2?")]
        public async Task OffsetFetchRequestOfNonExistingGroupShouldReturnNoError()
        {
            //From documentation: https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+ProtocolTests#AGuideToTheKafkaProtocol-OffsetFetchRequest
            //Note that if there is no offset associated with a topic-partition under that consumer group the broker does not set an error code
            //(since it is not really an error), but returns empty metadata and sets the offset field to -1.
            const int partitionId = 0;
            var router = new BrokerRouter(Options);

            var request = CreateOffsetFetchRequest(Guid.NewGuid().ToString(), partitionId);
            await router.GetTopicMetadataAsync(IntegrationConfig.IntegrationTopic, CancellationToken.None);
            var conn = router.GetBrokerRoute(IntegrationConfig.IntegrationTopic, partitionId);

            var response = await conn.Connection.SendAsync(request, CancellationToken.None);
            var topic = response.Topics.FirstOrDefault();

            Assert.That(topic, Is.Not.Null);
            Assert.That(topic.ErrorCode, Is.EqualTo((int)ErrorResponseCode.NoError));
            Assert.That(topic.Offset, Is.EqualTo(-1));
            router.Dispose();
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task OffsetCommitShouldStoreAndReturnSuccess()
        {
            const int partitionId = 0;
            var router = new BrokerRouter(Options);

            await router.GetTopicMetadataAsync(IntegrationConfig.IntegrationTopic, CancellationToken.None);
            var conn = router.GetBrokerRoute(IntegrationConfig.IntegrationTopic, partitionId);

            var commit = CreateOffsetCommitRequest(IntegrationConfig.IntegrationConsumer, partitionId, 10);
            var response = await conn.Connection.SendAsync(commit, CancellationToken.None);
            var topic = response.Topics.FirstOrDefault();

            Assert.That(topic, Is.Not.Null);
            Assert.That(topic.ErrorCode, Is.EqualTo(ErrorResponseCode.NoError));

            router.Dispose();
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task OffsetCommitShouldStoreOffsetValue()
        {
            const int partitionId = 0;
            const long offset = 99;

            var router = new BrokerRouter(Options);

            await router.GetTopicMetadataAsync(IntegrationConfig.IntegrationTopic, CancellationToken.None);
            var conn = router.GetBrokerRoute(IntegrationConfig.IntegrationTopic, partitionId);

            var commit = CreateOffsetCommitRequest(IntegrationConfig.IntegrationConsumer, partitionId, offset);
            var commitResponse = await conn.Connection.SendAsync(commit, CancellationToken.None);
            var commitTopic = commitResponse.Topics.SingleOrDefault();

            Assert.That(commitTopic, Is.Not.Null);
            Assert.That(commitTopic.ErrorCode, Is.EqualTo(ErrorResponseCode.NoError));

            var fetch = CreateOffsetFetchRequest(IntegrationConfig.IntegrationConsumer, partitionId);
            var fetchResponse = await conn.Connection.SendAsync(fetch, CancellationToken.None);
            var fetchTopic = fetchResponse.Topics.SingleOrDefault();

            Assert.That(fetchTopic, Is.Not.Null);
            Assert.That(fetchTopic.ErrorCode, Is.EqualTo(ErrorResponseCode.NoError));
            Assert.That(fetchTopic.Offset, Is.EqualTo(offset));
            router.Dispose();
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [Ignore("The response does not seem to return metadata information.  Not supported yet in kafka?")]
        public void OffsetCommitShouldStoreMetadata()
        {
            const int partitionId = 0;
            const long offset = 101;
            const string metadata = "metadata";

            var router = new BrokerRouter(Options);

            var conn = router.GetBrokerRoute(IntegrationConfig.IntegrationTopic, partitionId);

            var commit = CreateOffsetCommitRequest(IntegrationConfig.IntegrationConsumer, partitionId, offset, metadata);
            var commitResponse = conn.Connection.SendAsync(commit, CancellationToken.None).Result;
            var commitTopic = commitResponse.Topics.SingleOrDefault();

            Assert.That(commitTopic, Is.Not.Null);
            Assert.That(commitTopic.ErrorCode, Is.EqualTo(ErrorResponseCode.NoError));

            var fetch = CreateOffsetFetchRequest(IntegrationConfig.IntegrationConsumer, partitionId);
            var fetchResponse = conn.Connection.SendAsync(fetch, CancellationToken.None).Result;
            var fetchTopic = fetchResponse.Topics.SingleOrDefault();

            Assert.That(fetchTopic, Is.Not.Null);
            Assert.That(fetchTopic.ErrorCode, Is.EqualTo(ErrorResponseCode.NoError));
            Assert.That(fetchTopic.Offset, Is.EqualTo(offset));
            Assert.That(fetchTopic.MetaData, Is.EqualTo(metadata));
            router.Dispose();
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [Ignore("Not supported currently in 8.1.1?")]
        public void ConsumerMetadataRequestShouldReturnWithoutError()
        {
            using (var router = new BrokerRouter(Options))
            {
                var conn = router.GetBrokerRoute(IntegrationConfig.IntegrationTopic);

                var request = new GroupCoordinatorRequest(IntegrationConfig.IntegrationConsumer);

                var response = conn.Connection.SendAsync(request, CancellationToken.None).Result;

                Assert.That(response, Is.Not.Null);
                Assert.That(response.ErrorCode, Is.EqualTo((int)ErrorResponseCode.NoError));
            }
        }

        private OffsetFetchRequest CreateOffsetFetchRequest(string consumerGroup, int partitionId)
        {
            var request = new OffsetFetchRequest(consumerGroup, new Topic(IntegrationConfig.IntegrationTopic, partitionId));
            return request;
        }

        private OffsetCommitRequest CreateOffsetCommitRequest(string consumerGroup, int partitionId, long offset, string metadata = null)
        {
            var commit = new OffsetCommitRequest(consumerGroup, new []{ new OffsetCommit(IntegrationConfig.IntegrationTopic, partitionId, offset, metadata) });
            return commit;
        }
    }
}