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
using NSubstitute;
using NUnit.Framework;
#pragma warning disable 1998

namespace KafkaClient.Tests
{
    [TestFixture]
    [Category("Unit")]
    public class BrokerRouterTests
    {
        private const string TestTopic = BrokerRouterProxy.TestTopic;
        private IConnection _connection;
        private IConnectionFactory _connectionFactory;

        [SetUp]
        public void Setup()
        {
            //setup mock IConnection
            _connection = Substitute.For<IConnection>();
            _connectionFactory = Substitute.For<IConnectionFactory>();
            _connectionFactory
                .Create(Arg.Is<Endpoint>(e => e.IP.Port == 1), Arg.Any<IConnectionConfiguration>(), Arg.Any<ILog>())
                .Returns(_ => _connection);
            _connectionFactory
                .Resolve(Arg.Any<Uri>(), Arg.Any<ILog>())
                .Returns(_ => new Endpoint(_.Arg<Uri>(), new IPEndPoint(IPAddress.Parse("127.0.0.1"), _.Arg<Uri>().Port)));
        }

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public void BrokerRouterCanConstruct()
        {
            var result = new BrokerRouter(new Uri("http://localhost:1"), _connectionFactory);

            Assert.That(result, Is.Not.Null);
        }

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public void BrokerRouterConstructorThrowsException()
        {
            Assert.Throws<ConnectionException>(() => new BrokerRouter(new Uri("http://noaddress:1")));
        }

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public void BrokerRouterConstructorShouldIgnoreUnresolvableUriWhenAtLeastOneIsGood()
        {
            var result = new BrokerRouter(new [] { new Uri("http://noaddress:1"), new Uri("http://localhost:1") });
        }

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public async Task BrokerRouterUsesFactoryToAddNewBrokers()
        {
            var router = new BrokerRouter(new Uri("http://localhost:1"), _connectionFactory);

            _connection
                .SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                .Returns(_ => BrokerRouterProxy.CreateMetadataResponseWithMultipleBrokers());
            await router.GetTopicMetadataAsync(TestTopic, CancellationToken.None);
            var topics = router.GetTopicMetadata(TestTopic);
            _connectionFactory.Received()
                              .Create(Arg.Is<Endpoint>(e => e.IP.Port == 2), Arg.Any<IConnectionConfiguration>(),Arg.Any<ILog>());
        }

        #region MetadataRequest Tests...

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public async Task BrokerRouteShouldCycleThroughEachBrokerUntilOneIsFound()
        {
            var routerProxy = new BrokerRouterProxy();
            routerProxy.Connection1.MetadataResponseFunction = () => { throw new Exception("some error"); };
            var router = routerProxy.Create();
            await router.GetTopicMetadataAsync(TestTopic, CancellationToken.None);
            var result = router.GetTopicMetadata(TestTopic);
            Assert.That(result, Is.Not.Null);
            Assert.That(routerProxy.Connection1.MetadataRequestCallCount, Is.EqualTo(1));
            Assert.That(routerProxy.Connection2.MetadataRequestCallCount, Is.EqualTo(1));
        }

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public async Task BrokerRouteShouldThrowIfCycleCouldNotConnectToAnyServer()
        {
            var routerProxy = new BrokerRouterProxy();
            routerProxy.Connection1.MetadataResponseFunction = () => { throw new Exception("some error"); };
            routerProxy.Connection2.MetadataResponseFunction = () => { throw new Exception("some error"); };
            var router = routerProxy.Create();

            Assert.ThrowsAsync<RequestException>(async () => await router.GetTopicMetadataAsync(TestTopic, CancellationToken.None));

            Assert.That(routerProxy.Connection1.MetadataRequestCallCount, Is.EqualTo(1));
            Assert.That(routerProxy.Connection2.MetadataRequestCallCount, Is.EqualTo(1));
        }

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public async Task BrokerRouteShouldReturnTopicFromCache()
        {
            var routerProxy = new BrokerRouterProxy();
            var router = routerProxy.Create();
            await router.GetTopicMetadataAsync(TestTopic, CancellationToken.None);
            var result1 = router.GetTopicMetadata(TestTopic);
            var result2 = router.GetTopicMetadata(TestTopic);

            Assert.AreEqual(1, router.GetTopicMetadata().Count);
            Assert.That(routerProxy.Connection1.MetadataRequestCallCount, Is.EqualTo(1));
            Assert.That(result1.TopicName, Is.EqualTo(TestTopic));
            Assert.That(result2.TopicName, Is.EqualTo(TestTopic));
        }

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public async Task BrokerRouteShouldThrowNoLeaderElectedForPartition()
        {
            var routerProxy = new BrokerRouterProxy {
                MetadataResponse = BrokerRouterProxy.CreateMetadataResponseWithNotEndToElectLeader
            };

            var router = routerProxy.Create();
            Assert.ThrowsAsync<CachedMetadataException>(async () => await router.GetTopicMetadataAsync(TestTopic, CancellationToken.None));
            Assert.AreEqual(0, router.GetTopicMetadata().Count);
        }

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public async Task BrokerRouteShouldReturnAllTopicsFromCache()
        {
            var routerProxy = new BrokerRouterProxy();
            var router = routerProxy.Create();
            await router.RefreshTopicMetadataAsync(CancellationToken.None);
            var result1 = router.GetTopicMetadata();
            var result2 = router.GetTopicMetadata();

            Assert.That(routerProxy.Connection1.MetadataRequestCallCount, Is.EqualTo(1));
            Assert.That(result1.Count, Is.EqualTo(1));
            Assert.That(result1[0].TopicName, Is.EqualTo(TestTopic));
            Assert.That(result2.Count, Is.EqualTo(1));
            Assert.That(result2[0].TopicName, Is.EqualTo(TestTopic));
        }

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public async Task RefreshTopicMetadataShouldIgnoreCacheAndAlwaysCauseMetadataRequestAfterExpertionDate()
        {
            var routerProxy = new BrokerRouterProxy();
            var router = routerProxy.Create();
            TimeSpan cacheExpiration = TimeSpan.FromMilliseconds(100);
            await router.RefreshTopicMetadataAsync(TestTopic, true, CancellationToken.None);
            Assert.That(routerProxy.Connection1.MetadataRequestCallCount, Is.EqualTo(1));
            await Task.Delay(routerProxy.CacheExpiration);
            await Task.Delay(1);//After cache is expair
            await router.RefreshTopicMetadataAsync(TestTopic, true, CancellationToken.None);
            Assert.That(routerProxy.Connection1.MetadataRequestCallCount, Is.EqualTo(2));
        }

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public async Task RefreshAllTopicMetadataShouldAlwaysDoRequest()
        {
            var routerProxy = new BrokerRouterProxy();
            var router = routerProxy.Create();
            await router.RefreshTopicMetadataAsync(CancellationToken.None);
            Assert.That(routerProxy.Connection1.MetadataRequestCallCount, Is.EqualTo(1));
            await router.RefreshTopicMetadataAsync(CancellationToken.None);
            Assert.That(routerProxy.Connection1.MetadataRequestCallCount, Is.EqualTo(2));
        }

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public async Task SelectBrokerRouteShouldChange()
        {
            var routerProxy = new BrokerRouterProxy();

            var router = routerProxy.Create();

            routerProxy.MetadataResponse = BrokerRouterProxy.CreateMetadataResponseWithMultipleBrokers;
            await router.RefreshTopicMetadataAsync(TestTopic, true, CancellationToken.None);

            var router1 = router.GetBrokerRoute(TestTopic, 0);

            Assert.That(routerProxy.Connection1.MetadataRequestCallCount, Is.EqualTo(1));
            await Task.Delay(routerProxy.CacheExpiration);
            await Task.Delay(1);//After cache is expair
            routerProxy.MetadataResponse = BrokerRouterProxy.CreateMetadataResponseWithSingleBroker;
            await router.RefreshTopicMetadataAsync(TestTopic, true, CancellationToken.None);
            var router2 = router.GetBrokerRoute(TestTopic, 0);

            Assert.That(routerProxy.Connection1.MetadataRequestCallCount, Is.EqualTo(2));
            Assert.That(router1.Connection.Endpoint, Is.EqualTo(routerProxy.Connection1.Endpoint));
            Assert.That(router2.Connection.Endpoint, Is.EqualTo(routerProxy.Connection2.Endpoint));
            Assert.That(router1.Connection.Endpoint, Is.Not.EqualTo(router2.Connection.Endpoint));
        }

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public async Task SimultaneouslyRefreshTopicMetadataShouldNotGetDataFromCacheOnSameRequest()
        {
            var routerProxy = new BrokerRouterProxy();
            var router = routerProxy.Create();

            List<Task> x = new List<Task>();
            x.Add(router.RefreshTopicMetadataAsync(TestTopic, true, CancellationToken.None));//do not debug
            x.Add(router.RefreshTopicMetadataAsync(TestTopic, true, CancellationToken.None));//do not debug
            await Task.WhenAll(x.ToArray());
            Assert.That(routerProxy.Connection1.MetadataRequestCallCount, Is.EqualTo(2));
        }

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public async Task SimultaneouslyRefreshMissingTopicMetadataShouldGetDataFromCacheOnSameRequest()
        {
            var routerProxy = new BrokerRouterProxy();
            var router = routerProxy.Create();

            List<Task> x = new List<Task>();
            x.Add(router.GetTopicMetadataAsync(TestTopic, CancellationToken.None));//do not debug
            x.Add(router.GetTopicMetadataAsync(TestTopic, CancellationToken.None));//do not debug
            await Task.WhenAll(x.ToArray());
            Assert.That(routerProxy.Connection1.MetadataRequestCallCount, Is.EqualTo(1));
        }

        #endregion MetadataRequest Tests...

        #region SelectBrokerRouteAsync Exact Tests...

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public async Task SelectExactPartitionShouldReturnRequestedPartition()
        {
            var routerProxy = new BrokerRouterProxy();
            var router = routerProxy.Create();
            await router.GetTopicMetadataAsync(TestTopic, CancellationToken.None);
            var p0 = router.GetBrokerRoute(TestTopic, 0);
            var p1 = router.GetBrokerRoute(TestTopic, 1);

            Assert.That(p0.PartitionId, Is.EqualTo(0));
            Assert.That(p1.PartitionId, Is.EqualTo(1));
        }

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public async Task SelectExactPartitionShouldThrowWhenPartitionDoesNotExist()
        {
            var routerProxy = new BrokerRouterProxy();
            var router = routerProxy.Create();
            await router.GetTopicMetadataAsync(TestTopic, CancellationToken.None);
            Assert.Throws<CachedMetadataException>(() => router.GetBrokerRoute(TestTopic, 3));
        }

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public async Task SelectExactPartitionShouldThrowWhenTopicsCollectionIsEmpty()
        {
            var metadataResponse = await BrokerRouterProxy.CreateMetadataResponseWithMultipleBrokers();
            metadataResponse.Topics.Clear();

            var routerProxy = new BrokerRouterProxy();
#pragma warning disable 1998
            routerProxy.Connection1.MetadataResponseFunction = async () => metadataResponse;
#pragma warning restore 1998

            Assert.Throws<CachedMetadataException>(() => routerProxy.Create().GetBrokerRoute(TestTopic, 1));
        }

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public async Task SelectExactPartitionShouldThrowWhenBrokerCollectionIsEmpty()
        {
            var metadataResponse = await BrokerRouterProxy.CreateMetadataResponseWithMultipleBrokers();
            metadataResponse = new MetadataResponse(topics: metadataResponse.Topics);

            var routerProxy = new BrokerRouterProxy();
#pragma warning disable 1998
            routerProxy.Connection1.MetadataResponseFunction = async () => metadataResponse;
#pragma warning restore 1998
            var router = routerProxy.Create();
            await router.GetTopicMetadataAsync(TestTopic, CancellationToken.None);
            Assert.Throws<CachedMetadataException>(() => router.GetBrokerRoute(TestTopic, 1));
        }

        #endregion SelectBrokerRouteAsync Exact Tests...

        #region SelectBrokerRouteAsync Select Tests...

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        [TestCase(null)]
        [TestCase("withkey")]
        public async Task SelectPartitionShouldUsePartitionSelector(string testCase)
        {
            var key = testCase.ToIntSizedBytes();
            var routerProxy = new BrokerRouterProxy();

            var partitionSelector = Substitute.For<IPartitionSelector>();
            partitionSelector
                .Select(Arg.Any<MetadataResponse.Topic>(), key)
                .Returns(new MetadataResponse.Partition(0, 0, ErrorResponseCode.None, new[] { 1 }, new[] { 1 }));

            routerProxy.PartitionSelector = partitionSelector;
            var router = routerProxy.Create();
            await router.GetTopicMetadataAsync(TestTopic, CancellationToken.None);
            var result = router.GetBrokerRoute(TestTopic, key);

            partitionSelector.Received().Select(Arg.Is<MetadataResponse.Topic>(x => x.TopicName == TestTopic), key);
        }

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public async Task SelectPartitionShouldThrowWhenTopicsCollectionIsEmpty()
        {
            var metadataResponse = await BrokerRouterProxy.CreateMetadataResponseWithMultipleBrokers();
            metadataResponse.Topics.Clear();

            var routerProxy = new BrokerRouterProxy();
#pragma warning disable 1998
            routerProxy.Connection1.MetadataResponseFunction = async () => metadataResponse;
#pragma warning restore 1998

            Assert.Throws<CachedMetadataException>(() => routerProxy.Create().GetBrokerRoute(TestTopic));
        }

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public async Task SelectPartitionShouldThrowWhenBrokerCollectionIsEmpty()
        {
            var metadataResponse = await BrokerRouterProxy.CreateMetadataResponseWithMultipleBrokers();
            metadataResponse = new MetadataResponse(topics: metadataResponse.Topics);

            var routerProxy = new BrokerRouterProxy();
            var router = routerProxy.Connection1;
#pragma warning disable 1998
            routerProxy.Connection1.MetadataResponseFunction = async () => metadataResponse;
#pragma warning restore 1998
            var routerProxy1 = routerProxy.Create();
            await routerProxy1.GetTopicMetadataAsync(TestTopic, CancellationToken.None);
            Assert.Throws<CachedMetadataException>(() => routerProxy1.GetBrokerRoute(TestTopic));
        }

        #endregion SelectBrokerRouteAsync Select Tests...
    }
}