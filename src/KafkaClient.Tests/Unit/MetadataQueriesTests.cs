using System;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Protocol;
using NUnit.Framework;

namespace KafkaClient.Tests.Unit
{
    [TestFixture]
    public class MetadataQueriesUnitTests
    {

        #region GetTopicOffset Tests...

        [Test]
        public async Task GetTopicOffsetShouldQueryEachBroker()
        {
            var routerProxy = new BrokerRouterProxy();
            var router = routerProxy.Create();

            await router.GetTopicOffsetsAsync(BrokerRouterProxy.TestTopic, 2, -1, CancellationToken.None);
            Assert.That(routerProxy.Connection1[ApiKey.Offset], Is.EqualTo(1));
            Assert.That(routerProxy.Connection2[ApiKey.Offset], Is.EqualTo(1));
        }

        [Test]
        public async Task GetTopicOffsetShouldThrowAnyException()
        {
            var routerProxy = new BrokerRouterProxy();
            routerProxy.Connection1.Add(ApiKey.Offset, _ => { throw new Exception("test 99"); });
            var router = routerProxy.Create();

            try {
                await router.GetTopicOffsetsAsync(BrokerRouterProxy.TestTopic, 2,  -1, CancellationToken.None);
                Assert.Fail("Should have thrown exception");
            } catch (Exception ex) {
                Assert.That(ex.Message, Does.Contain("test 99"));
            }
        }

        #endregion GetTopicOffset Tests...

        #region GetTopic Tests...

        [Test]
        public async Task GetTopicShouldReturnTopic()
        {
            var routerProxy = new BrokerRouterProxy();
            var router = routerProxy.Create();
            await router.GetTopicMetadataAsync(BrokerRouterProxy.TestTopic, CancellationToken.None);

            var result = router.GetTopicMetadata(BrokerRouterProxy.TestTopic);
            Assert.That(result.TopicName, Is.EqualTo(BrokerRouterProxy.TestTopic));
        }

        [Test]
        public void EmptyTopicMetadataShouldThrowException()
        {
            var routerProxy = new BrokerRouterProxy();
            var router = routerProxy.Create();

            Assert.Throws<CachedMetadataException>(() => router.GetTopicMetadata("MissingTopic"));
        }

        #endregion GetTopic Tests...

    }
}