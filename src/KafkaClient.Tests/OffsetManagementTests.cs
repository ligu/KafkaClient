using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Protocol;
using KafkaClient.Tests.Helpers;
using NUnit.Framework;

namespace KafkaClient.Tests
{
    [TestFixture]
    [Category("Integration")]
    public class OffsetManagementTests
    {
        private readonly KafkaOptions _options = new KafkaOptions(TestConfig.IntegrationUri, log: TestConfig.InfoLog);

        [SetUp]
        public void Setup()
        {
        }

        [Test]
        public async Task OffsetFetchRequestOfNonExistingGroupShouldReturnNoError()
        {
            //From documentation: https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+ProtocolTests#AGuideToTheKafkaProtocol-OffsetFetchRequest
            //Note that if there is no offset associated with a topic-partition under that consumer group the broker does not set an error code
            //(since it is not really an error), but returns empty metadata and sets the offset field to -1.
            const int partitionId = 0;
            var router = new BrokerRouter(_options);

            var request = new OffsetFetchRequest(Guid.NewGuid().ToString(), new TopicPartition(TestConfig.TopicName(), partitionId));
            await router.GetTopicMetadataAsync(TestConfig.TopicName(), CancellationToken.None);
            var conn = router.GetBrokerRoute(TestConfig.TopicName(), partitionId);

            var response = await conn.Connection.SendAsync(request, CancellationToken.None);
            var topic = response.Topics.FirstOrDefault();

            Assert.That(topic, Is.Not.Null);
            Assert.That(topic.ErrorCode, Is.EqualTo(ErrorResponseCode.None));
            Assert.That(topic.Offset, Is.EqualTo(-1));
            router.Dispose();
        }

        [Test]
        public async Task OffsetCommitShouldStoreAndReturnSuccess()
        {
            const int partitionId = 0;
            var router = new BrokerRouter(_options);

            await router.GetTopicMetadataAsync(TestConfig.TopicName(), CancellationToken.None);
            var conn = router.GetBrokerRoute(TestConfig.TopicName(), partitionId);

            // ensure the group exists
            var group = new GroupCoordinatorRequest(TestConfig.ConsumerName());
            var groupResponse = await conn.Connection.SendAsync(group, CancellationToken.None);
            Assert.That(groupResponse, Is.Not.Null);
            Assert.That(groupResponse.ErrorCode, Is.EqualTo(ErrorResponseCode.None));

            var commit = new OffsetCommitRequest(group.GroupId, new []{ new OffsetCommitRequest.Topic(TestConfig.TopicName(), partitionId, 10, null) });
            var response = await conn.Connection.SendAsync(commit, CancellationToken.None);
            var topic = response.Topics.FirstOrDefault();

            Assert.That(topic, Is.Not.Null);
            Assert.That(topic.ErrorCode, Is.EqualTo(ErrorResponseCode.None));

            router.Dispose();
        }

        [Test]
        public async Task OffsetCommitShouldStoreOffsetValue()
        {
            const int partitionId = 0;
            const long offset = 99;

            var router = new BrokerRouter(_options);

            await router.GetTopicMetadataAsync(TestConfig.TopicName(), CancellationToken.None);
            var conn = router.GetBrokerRoute(TestConfig.TopicName(), partitionId);

            // ensure the group exists
            var group = new GroupCoordinatorRequest(TestConfig.ConsumerName());
            var groupResponse = await conn.Connection.SendAsync(group, CancellationToken.None);
            Assert.That(groupResponse, Is.Not.Null);
            Assert.That(groupResponse.ErrorCode, Is.EqualTo(ErrorResponseCode.None));

            var commit = new OffsetCommitRequest(group.GroupId, new []{ new OffsetCommitRequest.Topic(TestConfig.TopicName(), partitionId, offset, null) });
            var commitResponse = await conn.Connection.SendAsync(commit, CancellationToken.None);
            var commitTopic = commitResponse.Topics.SingleOrDefault();

            Assert.That(commitTopic, Is.Not.Null);
            Assert.That(commitTopic.ErrorCode, Is.EqualTo(ErrorResponseCode.None));

            var fetch = new OffsetFetchRequest(TestConfig.ConsumerName(), new TopicPartition(TestConfig.TopicName(), partitionId));
            var fetchResponse = await conn.Connection.SendAsync(fetch, CancellationToken.None);
            var fetchTopic = fetchResponse.Topics.SingleOrDefault();

            Assert.That(fetchTopic, Is.Not.Null);
            Assert.That(fetchTopic.ErrorCode, Is.EqualTo(ErrorResponseCode.None));
            Assert.That(fetchTopic.Offset, Is.EqualTo(offset));
            router.Dispose();
        }

        [Test]
        public async Task OffsetCommitShouldStoreMetadata()
        {
            const int partitionId = 0;
            const long offset = 101;
            const string metadata = "metadata";

            var router = new BrokerRouter(_options);

            var conn = await router.GetBrokerRouteAsync(TestConfig.TopicName(), partitionId, CancellationToken.None);

            // ensure the group exists
            var group = new GroupCoordinatorRequest(TestConfig.ConsumerName());
            var groupResponse = await conn.Connection.SendAsync(group, CancellationToken.None);
            Assert.That(groupResponse, Is.Not.Null);
            Assert.That(groupResponse.ErrorCode, Is.EqualTo(ErrorResponseCode.None));

            var commit = new OffsetCommitRequest(group.GroupId, new []{ new OffsetCommitRequest.Topic(TestConfig.TopicName(), partitionId, offset, metadata) });
            var commitResponse = await conn.Connection.SendAsync(commit, CancellationToken.None);
            var commitTopic = commitResponse.Topics.SingleOrDefault();

            Assert.That(commitTopic, Is.Not.Null);
            Assert.That(commitTopic.ErrorCode, Is.EqualTo(ErrorResponseCode.None));

            var fetch = new OffsetFetchRequest(TestConfig.ConsumerName(), commitTopic);
            var fetchResponse = await conn.Connection.SendAsync(fetch, CancellationToken.None);
            var fetchTopic = fetchResponse.Topics.SingleOrDefault();

            Assert.That(fetchTopic, Is.Not.Null);
            Assert.That(fetchTopic.ErrorCode, Is.EqualTo(ErrorResponseCode.None));
            Assert.That(fetchTopic.Offset, Is.EqualTo(offset));
            Assert.That(fetchTopic.MetaData, Is.EqualTo(metadata));
            router.Dispose();
        }

        [Test]
        public async Task ConsumerMetadataRequestShouldReturnWithoutError()
        {
            using (var router = new BrokerRouter(_options))
            {
                var conn = await router.GetBrokerRouteAsync(TestConfig.TopicName(), 0, CancellationToken.None);

                var request = new GroupCoordinatorRequest(TestConfig.ConsumerName());

                var response = await conn.Connection.SendAsync(request, CancellationToken.None);

                Assert.That(response, Is.Not.Null);
                Assert.That(response.ErrorCode, Is.EqualTo(ErrorResponseCode.None));
            }
        }
    }
}