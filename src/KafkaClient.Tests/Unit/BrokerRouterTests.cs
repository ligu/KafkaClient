using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Connections;
using KafkaClient.Protocol;
using KafkaClient.Tests.Fakes;
using KafkaClient.Tests.Helpers;
using Moq;
using Ninject.MockingKernel.Moq;
using NUnit.Framework;

namespace KafkaClient.Tests.Unit
{
    [TestFixture]
    [Category("Unit")]
    public class BrokerRouterTests
    {
        private const string TestTopic = BrokerRouterProxy.TestTopic;
        private MoqMockingKernel _kernel;
        private Mock<IConnection> _mockKafkaConnection1;
        private Mock<IConnectionFactory> _mockKafkaConnectionFactory;
        private Mock<IPartitionSelector> _mockPartitionSelector;

        [SetUp]
        public void Setup()
        {
            _kernel = new MoqMockingKernel();

            //setup mock IConnection
            _mockPartitionSelector = _kernel.GetMock<IPartitionSelector>();
            _mockKafkaConnection1 = _kernel.GetMock<IConnection>();
            _mockKafkaConnectionFactory = _kernel.GetMock<IConnectionFactory>();
            _mockKafkaConnectionFactory.Setup(x => x.Create(It.Is<Endpoint>(e => e.IP.Port == 1), It.IsAny<IConnectionConfiguration>(), It.IsAny<ILog>())).Returns(() => _mockKafkaConnection1.Object);
            _mockKafkaConnectionFactory.Setup(x => x.Resolve(It.IsAny<Uri>(), It.IsAny<ILog>()))
                .Returns<Uri, ILog>((uri, log) => new Endpoint(uri, new IPEndPoint(IPAddress.Parse("127.0.0.1"), uri.Port)));
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void BrokerRouterCanConstruct()
        {
            var result = new BrokerRouter(new Uri("http://localhost:1"), _mockKafkaConnectionFactory.Object);

            Assert.That(result, Is.Not.Null);
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [ExpectedException(typeof(ConnectionException))]
        public void BrokerRouterConstructorThrowsException()
        {
            var result = new BrokerRouter(new Uri("http://noaddress:1"));
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void BrokerRouterConstructorShouldIgnoreUnresolvableUriWhenAtLeastOneIsGood()
        {
            var result = new BrokerRouter(new [] { new Uri("http://noaddress:1"), new Uri("http://localhost:1") });
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task BrokerRouterUsesFactoryToAddNewBrokers()
        {
            var router = new BrokerRouter(new Uri("http://localhost:1"), _mockKafkaConnectionFactory.Object);

            _mockKafkaConnection1.Setup(x => x.SendAsync(It.IsAny<IRequest<MetadataResponse>>(), It.IsAny<CancellationToken>(), It.IsAny<IRequestContext>()))
                      .Returns(() => Task.Run(async () => await BrokerRouterProxy.CreateMetadataResponseWithMultipleBrokers()));
            await router.GetTopicMetadataAsync(TestTopic, CancellationToken.None);
            var topics = router.GetTopicMetadata(TestTopic);
            _mockKafkaConnectionFactory.Verify(x => x.Create(It.Is<Endpoint>(e => e.IP.Port == 2), It.IsAny<IConnectionConfiguration>(), It.IsAny<ILog>()), Times.Once());
        }

        #region MetadataRequest Tests...

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task BrokerRouteShouldCycleThroughEachBrokerUntilOneIsFound()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            routerProxy.BrokerConn0.MetadataResponseFunction = () => { throw new Exception("some error"); };
            var router = routerProxy.Create();
            await router.GetTopicMetadataAsync(TestTopic, CancellationToken.None);
            var result = router.GetTopicMetadata(TestTopic);
            Assert.That(result, Is.Not.Null);
            Assert.That(routerProxy.BrokerConn0.MetadataRequestCallCount, Is.EqualTo(1));
            Assert.That(routerProxy.BrokerConn1.MetadataRequestCallCount, Is.EqualTo(1));
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [ExpectedException(typeof(RequestException))]
        public async Task BrokerRouteShouldThrowIfCycleCouldNotConnectToAnyServer()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            routerProxy.BrokerConn0.MetadataResponseFunction = () => { throw new Exception("some error"); };
            routerProxy.BrokerConn1.MetadataResponseFunction = () => { throw new Exception("some error"); };
            var router = routerProxy.Create();

            try
            {
                await router.GetTopicMetadataAsync(TestTopic, CancellationToken.None);
                router.GetTopicMetadata(TestTopic);
            }
            catch
            {
                Assert.That(routerProxy.BrokerConn0.MetadataRequestCallCount, Is.EqualTo(1));
                Assert.That(routerProxy.BrokerConn1.MetadataRequestCallCount, Is.EqualTo(1));
                throw;
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task BrokerRouteShouldReturnTopicFromCache()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            var router = routerProxy.Create();
            await router.GetTopicMetadataAsync(TestTopic, CancellationToken.None);
            var result1 = router.GetTopicMetadata(TestTopic);
            var result2 = router.GetTopicMetadata(TestTopic);

            Assert.AreEqual(1, router.GetTopicMetadata().Count);
            Assert.That(routerProxy.BrokerConn0.MetadataRequestCallCount, Is.EqualTo(1));
            Assert.That(result1.TopicName, Is.EqualTo(TestTopic));
            Assert.That(result2.TopicName, Is.EqualTo(TestTopic));
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void BrokerRouteShouldThrowNoLeaderElectedForPartition()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            routerProxy.MetadataResponse = BrokerRouterProxy.CreateMetadataResponseWithNotEndToElectLeader;

            var router = routerProxy.Create();
            Assert.Throws<CachedMetadataException>(async () =>await router.GetTopicMetadataAsync(TestTopic, CancellationToken.None));
            Assert.AreEqual(0, router.GetTopicMetadata().Count);
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task BrokerRouteShouldReturnAllTopicsFromCache()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            var router = routerProxy.Create();
            await router.RefreshTopicMetadataAsync(CancellationToken.None);
            var result1 = router.GetTopicMetadata();
            var result2 = router.GetTopicMetadata();

            Assert.That(routerProxy.BrokerConn0.MetadataRequestCallCount, Is.EqualTo(1));
            Assert.That(result1.Count, Is.EqualTo(1));
            Assert.That(result1[0].TopicName, Is.EqualTo(TestTopic));
            Assert.That(result2.Count, Is.EqualTo(1));
            Assert.That(result2[0].TopicName, Is.EqualTo(TestTopic));
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task RefreshTopicMetadataShouldIgnoreCacheAndAlwaysCauseMetadataRequestAfterExpertionDate()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            var router = routerProxy.Create();
            TimeSpan cacheExpiration = TimeSpan.FromMilliseconds(100);
            await router.RefreshTopicMetadataAsync(TestTopic, true, CancellationToken.None);
            Assert.That(routerProxy.BrokerConn0.MetadataRequestCallCount, Is.EqualTo(1));
            await Task.Delay(routerProxy._cacheExpiration);
            await Task.Delay(1);//After cache is expair
            await router.RefreshTopicMetadataAsync(TestTopic, true, CancellationToken.None);
            Assert.That(routerProxy.BrokerConn0.MetadataRequestCallCount, Is.EqualTo(2));
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task RefreshAllTopicMetadataShouldAlwaysDoRequest()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            var router = routerProxy.Create();
            await router.RefreshTopicMetadataAsync(CancellationToken.None);
            Assert.That(routerProxy.BrokerConn0.MetadataRequestCallCount, Is.EqualTo(1));
            await router.RefreshTopicMetadataAsync(CancellationToken.None);
            Assert.That(routerProxy.BrokerConn0.MetadataRequestCallCount, Is.EqualTo(2));
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task SelectBrokerRouteShouldChange()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);

            var router = routerProxy.Create();

            routerProxy.MetadataResponse = BrokerRouterProxy.CreateMetadataResponseWithMultipleBrokers;
            await router.RefreshTopicMetadataAsync(TestTopic, true, CancellationToken.None);

            var router1 = router.GetBrokerRoute(TestTopic, 0);

            Assert.That(routerProxy.BrokerConn0.MetadataRequestCallCount, Is.EqualTo(1));
            await Task.Delay(routerProxy._cacheExpiration);
            await Task.Delay(1);//After cache is expair
            routerProxy.MetadataResponse = BrokerRouterProxy.CreateMetadataResponseWithSingleBroker;
            await router.RefreshTopicMetadataAsync(TestTopic, true, CancellationToken.None);
            var router2 = router.GetBrokerRoute(TestTopic, 0);

            Assert.That(routerProxy.BrokerConn0.MetadataRequestCallCount, Is.EqualTo(2));
            Assert.That(router1.Connection.Endpoint, Is.EqualTo(routerProxy.BrokerConn0.Endpoint));
            Assert.That(router2.Connection.Endpoint, Is.EqualTo(routerProxy.BrokerConn1.Endpoint));
            Assert.That(router1.Connection.Endpoint, Is.Not.EqualTo(router2.Connection.Endpoint));
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task SimultaneouslyRefreshTopicMetadataShouldNotGetDataFromCacheOnSameRequest()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            var router = routerProxy.Create();

            List<Task> x = new List<Task>();
            x.Add(router.RefreshTopicMetadataAsync(TestTopic, true, CancellationToken.None));//do not debug
            x.Add(router.RefreshTopicMetadataAsync(TestTopic, true, CancellationToken.None));//do not debug
            await Task.WhenAll(x.ToArray());
            Assert.That(routerProxy.BrokerConn0.MetadataRequestCallCount, Is.EqualTo(2));
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task SimultaneouslyRefreshMissingTopicMetadataShouldGetDataFromCacheOnSameRequest()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            var router = routerProxy.Create();

            List<Task> x = new List<Task>();
            x.Add(router.GetTopicMetadataAsync(TestTopic, CancellationToken.None));//do not debug
            x.Add(router.GetTopicMetadataAsync(TestTopic, CancellationToken.None));//do not debug
            await Task.WhenAll(x.ToArray());
            Assert.That(routerProxy.BrokerConn0.MetadataRequestCallCount, Is.EqualTo(1));
        }

        #endregion MetadataRequest Tests...

        #region SelectBrokerRouteAsync Exact Tests...

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task SelectExactPartitionShouldReturnRequestedPartition()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            var router = routerProxy.Create();
            await router.GetTopicMetadataAsync(TestTopic, CancellationToken.None);
            var p0 = router.GetBrokerRoute(TestTopic, 0);
            var p1 = router.GetBrokerRoute(TestTopic, 1);

            Assert.That(p0.PartitionId, Is.EqualTo(0));
            Assert.That(p1.PartitionId, Is.EqualTo(1));
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [ExpectedException(typeof(CachedMetadataException))]
        public async Task SelectExactPartitionShouldThrowWhenPartitionDoesNotExist()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            var router = routerProxy.Create();
            await router.GetTopicMetadataAsync(TestTopic, CancellationToken.None);
            router.GetBrokerRoute(TestTopic, 3);
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [ExpectedException(typeof(CachedMetadataException))]
        public async Task SelectExactPartitionShouldThrowWhenTopicsCollectionIsEmpty()
        {
            var metadataResponse = await BrokerRouterProxy.CreateMetadataResponseWithMultipleBrokers();
            metadataResponse.Topics.Clear();

            var routerProxy = new BrokerRouterProxy(_kernel);
#pragma warning disable 1998
            routerProxy.BrokerConn0.MetadataResponseFunction = async () => metadataResponse;
#pragma warning restore 1998

            routerProxy.Create().GetBrokerRoute(TestTopic, 1);
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [ExpectedException(typeof(CachedMetadataException))]
        public async Task SelectExactPartitionShouldThrowWhenBrokerCollectionIsEmpty()
        {
            var metadataResponse = await BrokerRouterProxy.CreateMetadataResponseWithMultipleBrokers();
            metadataResponse = new MetadataResponse(topics: metadataResponse.Topics);

            var routerProxy = new BrokerRouterProxy(_kernel);
#pragma warning disable 1998
            routerProxy.BrokerConn0.MetadataResponseFunction = async () => metadataResponse;
#pragma warning restore 1998
            var router = routerProxy.Create();
            await router.GetTopicMetadataAsync(TestTopic, CancellationToken.None);
            router.GetBrokerRoute(TestTopic, 1);
        }

        #endregion SelectBrokerRouteAsync Exact Tests...

        #region SelectBrokerRouteAsync Select Tests...

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [TestCase(null)]
        [TestCase("withkey")]
        public async Task SelectPartitionShouldUsePartitionSelector(string testCase)
        {
            var key = testCase.ToIntSizedBytes();
            var routerProxy = new BrokerRouterProxy(_kernel);

            _mockPartitionSelector.Setup(x => x.Select(It.IsAny<MetadataTopic>(), key))
                                  .Returns(() => new MetadataPartition(0, 0, ErrorResponseCode.NoError, new []{ 1 }, new []{ 1 }));

            routerProxy.PartitionSelector = _mockPartitionSelector.Object;
            var router = routerProxy.Create();
            await router.GetTopicMetadataAsync(TestTopic, CancellationToken.None);
            var result = router.GetBrokerRoute(TestTopic, key);

            _mockPartitionSelector.Verify(f => f.Select(It.Is<MetadataTopic>(x => x.TopicName == TestTopic), key), Times.Once());
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [ExpectedException(typeof(CachedMetadataException))]
        public async Task SelectPartitionShouldThrowWhenTopicsCollectionIsEmpty()
        {
            var metadataResponse = await BrokerRouterProxy.CreateMetadataResponseWithMultipleBrokers();
            metadataResponse.Topics.Clear();

            var routerProxy = new BrokerRouterProxy(_kernel);
#pragma warning disable 1998
            routerProxy.BrokerConn0.MetadataResponseFunction = async () => metadataResponse;
#pragma warning restore 1998

            routerProxy.Create().GetBrokerRoute(TestTopic);
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [ExpectedException(typeof(CachedMetadataException))]
        public async Task SelectPartitionShouldThrowWhenBrokerCollectionIsEmpty()
        {
            var metadataResponse = await BrokerRouterProxy.CreateMetadataResponseWithMultipleBrokers();
            metadataResponse = new MetadataResponse(topics: metadataResponse.Topics);

            var routerProxy = new BrokerRouterProxy(_kernel);
            var router = routerProxy.BrokerConn0;
#pragma warning disable 1998
            routerProxy.BrokerConn0.MetadataResponseFunction = async () => metadataResponse;
#pragma warning restore 1998
            var routerProxy1 = routerProxy.Create();
            await routerProxy1.GetTopicMetadataAsync(TestTopic, CancellationToken.None);
            routerProxy1.GetBrokerRoute(TestTopic);
        }

        #endregion SelectBrokerRouteAsync Select Tests...
    }
}