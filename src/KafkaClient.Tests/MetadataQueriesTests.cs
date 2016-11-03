using System;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Tests.Fakes;
using KafkaClient.Tests.Helpers;
using NUnit.Framework;

namespace KafkaClient.Tests
{
    [TestFixture]
    [Category("Unit")]
    public class MetadataQueriesTest
    {

        #region GetTopicOffset Tests...

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public void GetTopicOffsetShouldQueryEachBroker()
        {
            var routerProxy = new BrokerRouterProxy();
            var router = routerProxy.Create();

            var result = router.GetTopicOffsetsAsync(BrokerRouterProxy.TestTopic, 2, -1, CancellationToken.None).Result;
            Assert.That(routerProxy.Connection1.OffsetRequestCallCount, Is.EqualTo(1));
            Assert.That(routerProxy.Connection2.OffsetRequestCallCount, Is.EqualTo(1));
        }

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public void GetTopicOffsetShouldThrowAnyException()
        {
            var routerProxy = new BrokerRouterProxy();
            routerProxy.Connection1.OffsetResponseFunction = () => { throw new Exception("test 99"); };
            var router = routerProxy.Create();

            router.GetTopicOffsetsAsync(BrokerRouterProxy.TestTopic, 2,  -1, CancellationToken.None).ContinueWith(t =>
            {
                Assert.That(t.IsFaulted, Is.True);
                Assert.That(t.Exception.Flatten().ToString(), Does.Contain("test 99"));
            }).Wait();
        }

        #endregion GetTopicOffset Tests...

        #region GetTopic Tests...

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public async Task GetTopicShouldReturnTopic()
        {
            var routerProxy = new BrokerRouterProxy();
            var router = routerProxy.Create();
            await router.GetTopicMetadataAsync(BrokerRouterProxy.TestTopic, CancellationToken.None);

            var result = router.GetTopicMetadata(BrokerRouterProxy.TestTopic);
            Assert.That(result.TopicName, Is.EqualTo(BrokerRouterProxy.TestTopic));
        }

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public void EmptyTopicMetadataShouldThrowException()
        {
            var routerProxy = new BrokerRouterProxy();
            var router = routerProxy.Create();

            Assert.Throws<CachedMetadataException>(() => router.GetTopicMetadata("MissingTopic"));
        }

        #endregion GetTopic Tests...

    }
}