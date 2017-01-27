using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Connections;
using KafkaClient.Protocol;
using NSubstitute;
using NUnit.Framework;

#pragma warning disable 1998

namespace KafkaClient.Tests.Unit
{
    [TestFixture]
    public class RouterTests
    {
        private const string TestTopic = BrokerRouterProxy.TestTopic;

        [Test]
        public void BrokerRouterCanConstruct()
        {
            var connections = CreateConnections(1);
            var factory = CreateFactory(connections);

            var result = new Router(new Endpoint(new IPEndPoint(IPAddress.Loopback, 1)), factory);

            Assert.That(result, Is.Not.Null);
        }

        [Test]
        public void BrokerRouterConstructorThrowsException()
        {
            Assert.ThrowsAsync<ConnectionException>(() => Router.CreateAsync(new Uri("http://noaddress:1")));
        }

        [Test]
        public async Task BrokerRouterConstructorShouldIgnoreUnresolvableUriWhenAtLeastOneIsGood()
        {
            var result = await Router.CreateAsync(new [] { new Uri("http://noaddress:1"), new Uri("http://localhost:1") });
        }

        private IList<IConnection> CreateConnections(int count)
        {
            var connections = new List<IConnection>();
            for (var index = 0; index < count; index++) {
                var connection = Substitute.For<IConnection>();
                connection.Endpoint.Returns(new Endpoint(new IPEndPoint(IPAddress.Loopback, index), $"http://127.0.0.1:{index}"));
                connections.Add(connection);
            }
            return connections;
        }

        private IConnectionFactory CreateFactory(IEnumerable<IConnection> connections)
        {
            var factory = Substitute.For<IConnectionFactory>();
            factory
                .Create(Arg.Any<Endpoint>(), Arg.Any<IConnectionConfiguration>(), Arg.Any<ILog>())
                .Returns(_ => connections.SingleOrDefault(connection => connection.Endpoint == _.Arg<Endpoint>()));
            return factory;
        }

        [Test]
        public async Task BrokerRouterUsesFactoryToAddNewBrokers()
        {
            // Arrange
            var connections = CreateConnections(2);
            foreach (var connection in connections) {
                connection
                    .SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                    .Returns(_ => BrokerRouterProxy.CreateMetadataResponseWithMultipleBrokers());
            }
            var factory = CreateFactory(connections);
            var router = new Router(new Endpoint(new IPEndPoint(IPAddress.Loopback, 1)), factory);

            // Act
            await router.GetTopicMetadataAsync(TestTopic, CancellationToken.None);
            var topics = router.GetTopicMetadata(TestTopic);

            // Assert
            factory.Received()
                   .Create(Arg.Is<Endpoint>(e => e.Value.Port == 2), Arg.Any<IConnectionConfiguration>(), Arg.Any<ILog>());
        }

        [Test]
        public async Task BrokerRouterUsesFactoryToAddNewBrokersFromGroups()
        {
            // Arrange
            var connections = CreateConnections(2);
            foreach (var connection in connections) {
                connection
                    .SendAsync(Arg.Any<GroupCoordinatorRequest>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                    .Returns(_ => BrokerRouterProxy.CreateGroupCoordinatorResponse(1));
            }
            var factory = CreateFactory(connections);
            var router = new Router(new Endpoint(new IPEndPoint(IPAddress.Loopback, 1)), factory);

            // Act
            await router.GetGroupBrokerAsync(TestTopic, CancellationToken.None);
            var broker = router.GetGroupBroker(TestTopic);

            // Assert
            factory.Received()
                   .Create(Arg.Is<Endpoint>(e => e.Value.Port == 2), Arg.Any<IConnectionConfiguration>(), Arg.Any<ILog>());
        }

        #region Group Tests

        [Test]
        public async Task GetGroupShouldThrowWhenBrokerCollectionIsEmpty()
        {
            var routerProxy = new BrokerRouterProxy();
            var routerProxy1 = routerProxy.Create();
            Assert.Throws<CachedMetadataException>(() => routerProxy1.GetGroupBroker("unknown"));
        }

        [Test]
        public async Task BrokerRouteShouldCycleThroughEachBrokerUntilOneIsFoundForGroup()
        {
            var routerProxy = new BrokerRouterProxy();
            routerProxy.Connection1.Add(ApiKeyRequestType.GroupCoordinator, _ => { throw new Exception("some error"); });
            var router = routerProxy.Create();
            await router.GetGroupBrokerAsync(TestTopic, CancellationToken.None);
            var result = router.GetGroupBroker(TestTopic);
            Assert.That(result, Is.Not.Null);
            Assert.That(routerProxy.Connection1[ApiKeyRequestType.GroupCoordinator], Is.EqualTo(1));
            Assert.That(routerProxy.Connection2[ApiKeyRequestType.GroupCoordinator], Is.EqualTo(1));
        }

        [Test]
        public async Task BrokerRouteShouldThrowIfCycleCouldNotConnectToAnyServerForGroup()
        {
            var routerProxy = new BrokerRouterProxy();
            routerProxy.Connection1.Add(ApiKeyRequestType.GroupCoordinator, _ => { throw new Exception("some error"); });
            routerProxy.Connection2.Add(ApiKeyRequestType.GroupCoordinator, _ => { throw new Exception("some error"); });
            var router = routerProxy.Create();

            Assert.ThrowsAsync<CachedMetadataException>(async () => await router.GetGroupBrokerAsync(TestTopic, CancellationToken.None));

            Assert.That(routerProxy.Connection1[ApiKeyRequestType.GroupCoordinator], Is.EqualTo(1));
            Assert.That(routerProxy.Connection2[ApiKeyRequestType.GroupCoordinator], Is.EqualTo(1));
        }

        [Test]
        public async Task BrokerRouteShouldReturnGroupFromCache()
        {
            var routerProxy = new BrokerRouterProxy();
            var router = routerProxy.Create();
            await router.GetGroupBrokerAsync(TestTopic, CancellationToken.None);
            var result1 = router.GetGroupBroker(TestTopic);
            var result2 = router.GetGroupBroker(TestTopic);

            Assert.That(routerProxy.Connection1[ApiKeyRequestType.GroupCoordinator], Is.EqualTo(1));
            Assert.That(result1.GroupId, Is.EqualTo(TestTopic));
            Assert.That(result2.GroupId, Is.EqualTo(TestTopic));
        }

        [Test]
        public async Task RefreshGroupMetadataShouldIgnoreCacheAndAlwaysCauseRequestAfterExpirationDate()
        {
            var routerProxy = new BrokerRouterProxy();
            var router = routerProxy.Create();
            TimeSpan cacheExpiration = TimeSpan.FromMilliseconds(100);
            await router.RefreshGroupBrokerAsync(TestTopic, true, CancellationToken.None);
            Assert.That(routerProxy.Connection1[ApiKeyRequestType.GroupCoordinator], Is.EqualTo(1));
            await Task.Delay(routerProxy.CacheExpiration);
            await Task.Delay(1);//After cache is expired
            await router.RefreshGroupBrokerAsync(TestTopic, true, CancellationToken.None);
            Assert.That(routerProxy.Connection1[ApiKeyRequestType.GroupCoordinator], Is.EqualTo(2));
        }

        [Test]
        public async Task SimultaneouslyRefreshGroupMetadataShouldNotGetDataFromCacheOnSameRequest()
        {
            var routerProxy = new BrokerRouterProxy();
            var router = routerProxy.Create();

            List<Task> x = new List<Task>();
            x.Add(router.RefreshGroupBrokerAsync(TestTopic, true, CancellationToken.None));//do not debug
            x.Add(router.RefreshGroupBrokerAsync(TestTopic, true, CancellationToken.None));//do not debug
            await Task.WhenAll(x.ToArray());
            Assert.That(routerProxy.Connection1[ApiKeyRequestType.GroupCoordinator], Is.EqualTo(2));
        }

        [Test]
        public async Task SimultaneouslyGetGroupMetadataShouldGetDataFromCacheOnSameRequest()
        {
            var routerProxy = new BrokerRouterProxy();
            var router = routerProxy.Create();

            List<Task> x = new List<Task>();
            x.Add(router.GetGroupBrokerAsync(TestTopic, CancellationToken.None));//do not debug
            x.Add(router.GetGroupBrokerAsync(TestTopic, CancellationToken.None));//do not debug
            await Task.WhenAll(x.ToArray());
            Assert.That(routerProxy.Connection1[ApiKeyRequestType.GroupCoordinator], Is.EqualTo(1));
        }

        #endregion

        #region MetadataRequest Tests...

        [Test]
        public async Task BrokerRouteShouldCycleThroughEachBrokerUntilOneIsFound()
        {
            var routerProxy = new BrokerRouterProxy();
            routerProxy.Connection1.Add(ApiKeyRequestType.Metadata, _ => { throw new Exception("some error"); });
            var router = routerProxy.Create();
            await router.GetTopicMetadataAsync(TestTopic, CancellationToken.None);
            var result = router.GetTopicMetadata(TestTopic);
            Assert.That(result, Is.Not.Null);
            Assert.That(routerProxy.Connection1[ApiKeyRequestType.Metadata], Is.EqualTo(1));
            Assert.That(routerProxy.Connection2[ApiKeyRequestType.Metadata], Is.EqualTo(1));
        }

        [Test]
        public async Task BrokerRouteShouldThrowIfCycleCouldNotConnectToAnyServer()
        {
            var routerProxy = new BrokerRouterProxy();
            routerProxy.Connection1.Add(ApiKeyRequestType.Metadata, _ => { throw new Exception("some error"); });
            routerProxy.Connection2.Add(ApiKeyRequestType.Metadata, _ => { throw new Exception("some error"); });
            var router = routerProxy.Create();

            Assert.ThrowsAsync<ConnectionException>(async () => await router.GetTopicMetadataAsync(TestTopic, CancellationToken.None));

            Assert.That(routerProxy.Connection1[ApiKeyRequestType.Metadata], Is.EqualTo(1));
            Assert.That(routerProxy.Connection2[ApiKeyRequestType.Metadata], Is.EqualTo(1));
        }

        [Test]
        public async Task BrokerRouteShouldReturnTopicFromCache()
        {
            var routerProxy = new BrokerRouterProxy();
            var router = routerProxy.Create();
            await router.GetTopicMetadataAsync(TestTopic, CancellationToken.None);
            var result1 = router.GetTopicMetadata(TestTopic);
            var result2 = router.GetTopicMetadata(TestTopic);

            Assert.AreEqual(1, router.GetTopicMetadata().Count);
            Assert.That(routerProxy.Connection1[ApiKeyRequestType.Metadata], Is.EqualTo(1));
            Assert.That(result1.TopicName, Is.EqualTo(TestTopic));
            Assert.That(result2.TopicName, Is.EqualTo(TestTopic));
        }

        [Test]
        public async Task BrokerRouteShouldThrowNoLeaderElectedForPartition()
        {
            var routerProxy = new BrokerRouterProxy {
                MetadataResponse = BrokerRouterProxy.CreateMetadataResponseWithNotEndToElectLeader
            };

            var router = routerProxy.Create();
            Assert.ThrowsAsync<CachedMetadataException>(async () => await router.GetTopicMetadataAsync(TestTopic, CancellationToken.None));
            Assert.AreEqual(0, router.GetTopicMetadata().Count);
        }

        [Test]
        public async Task BrokerRouteShouldReturnAllTopicsFromCache()
        {
            var routerProxy = new BrokerRouterProxy();
            var router = routerProxy.Create();
            await router.RefreshTopicMetadataAsync(CancellationToken.None);
            var result1 = router.GetTopicMetadata();
            var result2 = router.GetTopicMetadata();

            Assert.That(routerProxy.Connection1[ApiKeyRequestType.Metadata], Is.EqualTo(1));
            Assert.That(result1.Count, Is.EqualTo(1));
            Assert.That(result1[0].TopicName, Is.EqualTo(TestTopic));
            Assert.That(result2.Count, Is.EqualTo(1));
            Assert.That(result2[0].TopicName, Is.EqualTo(TestTopic));
        }

        [Test]
        public async Task RefreshTopicMetadataShouldIgnoreCacheAndAlwaysCauseMetadataRequestAfterExpirationDate()
        {
            var routerProxy = new BrokerRouterProxy();
            var router = routerProxy.Create();
            TimeSpan cacheExpiration = TimeSpan.FromMilliseconds(100);
            await router.RefreshTopicMetadataAsync(TestTopic, true, CancellationToken.None);
            Assert.That(routerProxy.Connection1[ApiKeyRequestType.Metadata], Is.EqualTo(1));
            await Task.Delay(routerProxy.CacheExpiration);
            await Task.Delay(1);//After cache is expired
            await router.RefreshTopicMetadataAsync(TestTopic, true, CancellationToken.None);
            Assert.That(routerProxy.Connection1[ApiKeyRequestType.Metadata], Is.EqualTo(2));
        }

        [Test]
        public async Task RefreshAllTopicMetadataShouldAlwaysDoRequest()
        {
            var routerProxy = new BrokerRouterProxy();
            var router = routerProxy.Create();
            await router.RefreshTopicMetadataAsync(CancellationToken.None);
            Assert.That(routerProxy.Connection1[ApiKeyRequestType.Metadata], Is.EqualTo(1));
            await router.RefreshTopicMetadataAsync(CancellationToken.None);
            Assert.That(routerProxy.Connection1[ApiKeyRequestType.Metadata], Is.EqualTo(2));
        }

        [Test]
        public async Task SelectBrokerRouteShouldChange()
        {
            var routerProxy = new BrokerRouterProxy();

            var router = routerProxy.Create();

            routerProxy.MetadataResponse = BrokerRouterProxy.CreateMetadataResponseWithMultipleBrokers;
            await router.RefreshTopicMetadataAsync(TestTopic, true, CancellationToken.None);

            var router1 = router.GetTopicBroker(TestTopic, 0);

            Assert.That(routerProxy.Connection1[ApiKeyRequestType.Metadata], Is.EqualTo(1));
            await Task.Delay(routerProxy.CacheExpiration);
            await Task.Delay(1);//After cache is expired
            routerProxy.MetadataResponse = BrokerRouterProxy.CreateMetadataResponseWithSingleBroker;
            await router.RefreshTopicMetadataAsync(TestTopic, true, CancellationToken.None);
            var router2 = router.GetTopicBroker(TestTopic, 0);

            Assert.That(routerProxy.Connection1[ApiKeyRequestType.Metadata], Is.EqualTo(2));
            Assert.That(router1.Connection.Endpoint, Is.EqualTo(routerProxy.Connection1.Endpoint));
            Assert.That(router2.Connection.Endpoint, Is.EqualTo(routerProxy.Connection2.Endpoint));
            Assert.That(router1.Connection.Endpoint, Is.Not.EqualTo(router2.Connection.Endpoint));
        }

        [Test]
        public async Task SimultaneouslyRefreshTopicMetadataShouldNotGetDataFromCacheOnSameRequest()
        {
            var routerProxy = new BrokerRouterProxy();
            var router = routerProxy.Create();

            List<Task> x = new List<Task>();
            x.Add(router.RefreshTopicMetadataAsync(TestTopic, true, CancellationToken.None));//do not debug
            x.Add(router.RefreshTopicMetadataAsync(TestTopic, true, CancellationToken.None));//do not debug
            await Task.WhenAll(x.ToArray());
            Assert.That(routerProxy.Connection1[ApiKeyRequestType.Metadata], Is.EqualTo(2));
        }

        [Test]
        public async Task SimultaneouslyGetTopicMetadataShouldGetDataFromCacheOnSameRequest()
        {
            var routerProxy = new BrokerRouterProxy();
            var router = routerProxy.Create();

            List<Task> x = new List<Task>();
            x.Add(router.GetTopicMetadataAsync(TestTopic, CancellationToken.None));//do not debug
            x.Add(router.GetTopicMetadataAsync(TestTopic, CancellationToken.None));//do not debug
            await Task.WhenAll(x.ToArray());
            Assert.That(routerProxy.Connection1[ApiKeyRequestType.Metadata], Is.EqualTo(1));
        }

        #endregion MetadataRequest Tests...

        #region SelectBrokerRouteAsync Exact Tests...

        [Test]
        public async Task SelectExactPartitionShouldReturnRequestedPartition()
        {
            var routerProxy = new BrokerRouterProxy();
            var router = routerProxy.Create();
            await router.GetTopicMetadataAsync(TestTopic, CancellationToken.None);
            var p0 = router.GetTopicBroker(TestTopic, 0);
            var p1 = router.GetTopicBroker(TestTopic, 1);

            Assert.That(p0.PartitionId, Is.EqualTo(0));
            Assert.That(p1.PartitionId, Is.EqualTo(1));
        }

        [Test]
        public async Task SelectExactPartitionShouldThrowWhenPartitionDoesNotExist()
        {
            var routerProxy = new BrokerRouterProxy();
            var router = routerProxy.Create();
            await router.GetTopicMetadataAsync(TestTopic, CancellationToken.None);
            Assert.Throws<CachedMetadataException>(() => router.GetTopicBroker(TestTopic, 3));
        }

        [Test]
        public async Task SelectExactPartitionShouldThrowWhenTopicsCollectionIsEmpty()
        {
            var metadataResponse = await BrokerRouterProxy.CreateMetadataResponseWithMultipleBrokers();
            metadataResponse.Topics.Clear();

            var routerProxy = new BrokerRouterProxy();
#pragma warning disable 1998
            routerProxy.Connection1.Add(ApiKeyRequestType.Metadata, async _ => metadataResponse);
#pragma warning restore 1998

            Assert.Throws<CachedMetadataException>(() => routerProxy.Create().GetTopicBroker(TestTopic, 1));
        }

        [Test]
        public async Task SelectExactPartitionShouldThrowWhenBrokerCollectionIsEmpty()
        {
            var metadataResponse = await BrokerRouterProxy.CreateMetadataResponseWithMultipleBrokers();
            metadataResponse = new MetadataResponse(topics: metadataResponse.Topics);

            var routerProxy = new BrokerRouterProxy();
#pragma warning disable 1998
            routerProxy.Connection1.Add(ApiKeyRequestType.Metadata, async _ => metadataResponse);
#pragma warning restore 1998
            var router = routerProxy.Create();
            await router.GetTopicMetadataAsync(TestTopic, CancellationToken.None);
            Assert.Throws<CachedMetadataException>(() => router.GetTopicBroker(TestTopic, 1));
        }

        #endregion SelectBrokerRouteAsync Exact Tests...
       
    }
}