using System;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Tests.Fakes;
using KafkaClient.Tests.Helpers;
using Moq;
using Ninject.MockingKernel.Moq;
using NUnit.Framework;

namespace KafkaClient.Tests.Unit
{
    [TestFixture]
    [Category("Unit")]
    public class MetadataQueriesTest
    {
        private MoqMockingKernel _kernel;

        [SetUp]
        public void Setup()
        {
            _kernel = new MoqMockingKernel();
        }

        #region GetTopicOffset Tests...

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void GetTopicOffsetShouldQueryEachBroker()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            var router = routerProxy.Create();

            var result = router.GetTopicOffsetAsync(BrokerRouterProxy.TestTopic, 2, -1, CancellationToken.None).Result;
            Assert.That(routerProxy.BrokerConn0.OffsetRequestCallCount, Is.EqualTo(1));
            Assert.That(routerProxy.BrokerConn1.OffsetRequestCallCount, Is.EqualTo(1));
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void GetTopicOffsetShouldThrowAnyException()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            routerProxy.BrokerConn0.OffsetResponseFunction = () => { throw new ApplicationException("test 99"); };
            var router = routerProxy.Create();

            router.GetTopicOffsetAsync(BrokerRouterProxy.TestTopic, 2,  -1, CancellationToken.None).ContinueWith(t =>
            {
                Assert.That(t.IsFaulted, Is.True);
                Assert.That(t.Exception.Flatten().ToString(), Is.StringContaining("test 99"));
            }).Wait();
        }

        #endregion GetTopicOffset Tests...

        #region GetTopic Tests...

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task GetTopicShouldReturnTopic()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            var router = routerProxy.Create();
            await router.GetTopicMetadataAsync(BrokerRouterProxy.TestTopic, CancellationToken.None);

            var result = router.GetTopicMetadata(BrokerRouterProxy.TestTopic);
            Assert.That(result.TopicName, Is.EqualTo(BrokerRouterProxy.TestTopic));
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [ExpectedException(typeof(CachedMetadataException))]
        public void EmptyTopicMetadataShouldThrowException()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            var router = routerProxy.Create();

            router.GetTopicMetadata("MissingTopic");
        }

        #endregion GetTopic Tests...

    }
}