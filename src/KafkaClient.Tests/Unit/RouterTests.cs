﻿using System;
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
        #region SendAsync

        [TestCase(ErrorCode.NOT_LEADER_FOR_PARTITION)]
        [TestCase(ErrorCode.LEADER_NOT_AVAILABLE)]
        [TestCase(ErrorCode.GROUP_COORDINATOR_NOT_AVAILABLE)]
        [TestCase(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION)]
        [Test]
        public async Task ShouldTryToRefreshMataDataIfCanRecoverByRefreshMetadata(ErrorCode code)
        {
            var scenario = new RoutingScenario();
            var cacheExpiration = new TimeSpan(10);
            var router = scenario.CreateRouter(cacheExpiration);

            scenario.Connection1.Add(ApiKey.Fetch, FailedInFirstMessageError(code, cacheExpiration));
            scenario.Connection1.Add(ApiKey.Metadata, async _ => await RoutingScenario.DefaultMetadataResponse());

            await router.SendAsync(new FetchRequest(), RoutingScenario.TestTopic, 0, CancellationToken.None);

            Assert.That(scenario.Connection1[ApiKey.Metadata], Is.EqualTo(2));
            Assert.That(scenario.Connection1[ApiKey.Fetch], Is.EqualTo(2));
        }

        [Test]
        [TestCase(typeof(ConnectionException))]
        [TestCase(typeof(FetchOutOfRangeException))]
        [TestCase(typeof(RoutingException))]
        public async Task ShouldTryToRefreshMataDataIfOnExceptions(Type exceptionType)
        {
            var scenario = new RoutingScenario();
            var cacheExpiration = TimeSpan.FromMilliseconds(10);
            var router = scenario.CreateRouter(cacheExpiration);

            scenario.Connection1.Add(ApiKey.Fetch, FailedInFirstMessageException(exceptionType, cacheExpiration));
            scenario.Connection1.Add(ApiKey.Metadata, async _ => await RoutingScenario.DefaultMetadataResponse());

            await router.SendAsync(new FetchRequest(), RoutingScenario.TestTopic, 0, CancellationToken.None);

            Assert.That(scenario.Connection1[ApiKey.Metadata], Is.EqualTo(2));
            Assert.That(scenario.Connection1[ApiKey.Fetch], Is.EqualTo(2));
        }

        [TestCase(typeof(Exception))]
        [TestCase(typeof(RequestException))]
        public async Task SendProtocolRequestShouldThrowException(Type exceptionType)
        {
            var scenario = new RoutingScenario();
            var cacheExpiration = TimeSpan.FromMilliseconds(10);
            var router = scenario.CreateRouter(cacheExpiration);

            scenario.Connection1.Add(ApiKey.Fetch, FailedInFirstMessageException(exceptionType, cacheExpiration));
            scenario.Connection1.Add(ApiKey.Metadata, async _ => await RoutingScenario.DefaultMetadataResponse());
            Assert.ThrowsAsync(exceptionType, async () => await router.SendAsync(new FetchRequest(), RoutingScenario.TestTopic, 0, CancellationToken.None));
        }

        [Test]
        [TestCase(ErrorCode.INVALID_FETCH_SIZE)]
        [TestCase(ErrorCode.MESSAGE_TOO_LARGE)]
        [TestCase(ErrorCode.OFFSET_METADATA_TOO_LARGE)]
        [TestCase(ErrorCode.OFFSET_OUT_OF_RANGE)]
        [TestCase(ErrorCode.UNKNOWN)]
        [TestCase(ErrorCode.STALE_CONTROLLER_EPOCH)]
        [TestCase(ErrorCode.REPLICA_NOT_AVAILABLE)]
        public async Task SendProtocolRequestShouldNotTryToRefreshMataDataIfCanNotRecoverByRefreshMetadata(
            ErrorCode code)
        {
            var scenario = new RoutingScenario();
            var cacheExpiration = TimeSpan.FromMilliseconds(10);
            var router = scenario.CreateRouter(cacheExpiration);

            scenario.Connection1.Add(ApiKey.Fetch, FailedInFirstMessageError(code, cacheExpiration));
            scenario.Connection1.Add(ApiKey.Metadata, async _ => await RoutingScenario.DefaultMetadataResponse());
            Assert.ThrowsAsync<RequestException>(async () => await router.SendAsync(new FetchRequest(), RoutingScenario.TestTopic, 0, CancellationToken.None));
        }

        [Test]
        public async Task ShouldUpdateMetadataOnce()
        {
            var scenario = new RoutingScenario();
            var cacheExpiration = TimeSpan.FromMilliseconds(100);
            var router = scenario.CreateRouter(cacheExpiration);

            scenario.Connection1.Add(ApiKey.Fetch, ShouldReturnValidMessage);
            scenario.Connection1.Add(ApiKey.Metadata, async _ => await RoutingScenario.DefaultMetadataResponse());
            int numberOfCall = 1000;
            Task[] tasks = new Task[numberOfCall];
            for (int i = 0; i < numberOfCall / 2; i++)
            {
                tasks[i] = router.SendAsync(new FetchRequest(), RoutingScenario.TestTopic, 0, CancellationToken.None);
            }
            await Task.Delay(cacheExpiration);
            await Task.Delay(1);
            for (int i = 0; i < numberOfCall / 2; i++)
            {
                tasks[i + numberOfCall / 2] = router.SendAsync(new FetchRequest(), RoutingScenario.TestTopic, 0, CancellationToken.None);
            }

            await Task.WhenAll(tasks);
            Assert.That(scenario.Connection1[ApiKey.Fetch], Is.EqualTo(numberOfCall));
            Assert.That(scenario.Connection1[ApiKey.Metadata], Is.EqualTo(1));
        }

        [Test]
        public async Task ShouldRecoverUpdateMetadataForNewTopic()
        {
            var scenario = new RoutingScenario();
            var cacheExpiration = TimeSpan.FromMilliseconds(100);
            var router = scenario.CreateRouter(cacheExpiration);

            var fetchRequest = new FetchRequest();

            scenario.Connection1.Add(ApiKey.Fetch, ShouldReturnValidMessage);
            scenario.Connection1.Add(ApiKey.Metadata, async _ => await RoutingScenario.DefaultMetadataResponse());
            int numberOfCall = 100;
            Task[] tasks = new Task[numberOfCall];
            for (int i = 0; i < numberOfCall / 2; i++)
            {
                tasks[i] = router.SendAsync(fetchRequest, RoutingScenario.TestTopic, 0, CancellationToken.None);
            }

            scenario.Connection1.Add(ApiKey.Metadata, async _ => {
                var response = await RoutingScenario.DefaultMetadataResponse();
                return new MetadataResponse(response.Brokers, response.Topics.Select(t => new MetadataResponse.Topic("test2", t.ErrorCode, t.Partitions)));
            });

            for (int i = 0; i < numberOfCall / 2; i++)
            {
                tasks[i + numberOfCall / 2] = router.SendAsync(fetchRequest, "test2", 0, CancellationToken.None);
            }

            await Task.WhenAll(tasks);
            Assert.That(scenario.Connection1[ApiKey.Fetch], Is.EqualTo(numberOfCall));
            Assert.That(scenario.Connection1[ApiKey.Metadata], Is.EqualTo(2));
        }

        [Test]
        public async Task ShouldRecoverFromFailureByUpdateMetadataOnce() //Do not debug this test !!
        {
            var scenario = new RoutingScenario();
            var cacheExpiration = TimeSpan.FromMilliseconds(1000);
            var router = scenario.CreateRouter(cacheExpiration);

            int partitionId = 0;
            var fetchRequest = new FetchRequest();

            int numberOfCall = 100;
            long numberOfErrorSend = 0;
            TaskCompletionSource<int> x = new TaskCompletionSource<int>();
            Func<IRequestContext, Task<IResponse>> ShouldReturnNotLeaderForPartitionAndThenNoError = async _ =>
            {
                var log = TestConfig.Log;
                log.Debug(() => LogEvent.Create("FetchResponse Start "));
                if (!x.Task.IsCompleted)
                {
                    if (Interlocked.Increment(ref numberOfErrorSend) == numberOfCall)
                    {
                        await Task.Delay(cacheExpiration);
                        await Task.Delay(1);
                        x.TrySetResult(1);
                        log.Debug(() => LogEvent.Create("all is complete "));
                    }

                    await x.Task;
                    log.Debug(() => LogEvent.Create("SocketException "));
                    throw new ConnectionException(scenario.Connection1.Endpoint);
                }
                log.Debug(() => LogEvent.Create("Completed "));

                return new FetchResponse();
            };

            scenario.Connection1.Add(ApiKey.Fetch, ShouldReturnNotLeaderForPartitionAndThenNoError);
            scenario.Connection1.Add(ApiKey.Metadata, async _ => await RoutingScenario.DefaultMetadataResponse());

            Task[] tasks = new Task[numberOfCall];

            for (int i = 0; i < numberOfCall; i++)
            {
                tasks[i] = router.SendAsync(fetchRequest, RoutingScenario.TestTopic, partitionId, CancellationToken.None);
            }

            await Task.WhenAll(tasks);
            Assert.That(numberOfErrorSend, Is.GreaterThan(1), "numberOfErrorSend");
            Assert.That(scenario.Connection1[ApiKey.Fetch], Is.EqualTo(numberOfCall + numberOfErrorSend),
                "RequestCallCount(ApiKey.Fetch)");
            Assert.That(scenario.Connection1[ApiKey.Metadata], Is.EqualTo(2), "RequestCallCount(ApiKey.Metadata)");
        }

        [Test]
        public async Task ShouldRecoverFromConnectionExceptionByUpdateMetadataOnceFullScenario() //Do not debug this test !!
        {
            await ShouldRecoverByUpdateMetadataOnceFullScenario(
                FailedInFirstMessageException(typeof(ConnectionException), TimeSpan.Zero));
        }

        [Test]
        public async Task ShouldRecoverFromFetchErrorByUpdateMetadataOnceFullScenario1()
        {
            await ShouldRecoverByUpdateMetadataOnceFullScenario(
                FailedInFirstMessageError(ErrorCode.LEADER_NOT_AVAILABLE, TimeSpan.Zero));
        }

        /// <summary>
        /// Do not debug this test !!
        /// </summary>
        private async Task ShouldRecoverByUpdateMetadataOnceFullScenario(Func<IRequestContext, Task<IResponse>> fetchResponse)
        {
            var scenario = new RoutingScenario();
            var cacheExpiration = TimeSpan.Zero;
            var router = scenario.CreateRouter(cacheExpiration);
            int partitionId = 0;
            var fetchRequest = new FetchRequest();

            CreateSuccessfulSendMock(scenario);

            //Send Successful Message
            await router.SendAsync(fetchRequest, RoutingScenario.TestTopic, partitionId, CancellationToken.None);

            Assert.That(scenario.Connection1[ApiKey.Fetch], Is.EqualTo(1), "RequestCallCount(ApiKey.Fetch)");
            Assert.That(scenario.Connection1[ApiKey.Metadata], Is.EqualTo(1), "RequestCallCount(ApiKey.Metadata)");
            Assert.That(scenario.Connection2[ApiKey.Metadata], Is.EqualTo(0), "RequestCallCount(ApiKey.Metadata)");

            scenario.Connection1.Add(ApiKey.Fetch, fetchResponse);
            //triger to update metadata
            scenario.Connection1.Add(ApiKey.Metadata, async _ => await RoutingScenario.MetaResponseWithException());
            scenario.Connection2.Add(ApiKey.Metadata, async _ => await RoutingScenario.MetadataResponseWithSingleBroker());

            //Reset variables
            scenario.Connection1[ApiKey.Fetch] = 0;
            scenario.Connection2[ApiKey.Fetch] = 0;
            scenario.Connection1[ApiKey.Metadata] = 0;
            scenario.Connection2[ApiKey.Metadata] = 0;

            //Send Successful Message that was recover from exception
            await router.SendAsync(fetchRequest, RoutingScenario.TestTopic, partitionId, CancellationToken.None);

            Assert.That(scenario.Connection1[ApiKey.Fetch], Is.EqualTo(1), "RequestCallCount(ApiKey.Fetch)");
            Assert.That(scenario.Connection1[ApiKey.Metadata], Is.EqualTo(1), "RequestCallCount(ApiKey.Metadata)");

            Assert.That(scenario.Connection2[ApiKey.Fetch], Is.EqualTo(1), "RequestCallCount(ApiKey.Fetch)");
            Assert.That(scenario.Connection2[ApiKey.Metadata], Is.EqualTo(1), "RequestCallCount(ApiKey.Metadata)");
        }


        private static Func<IRequestContext, Task<IResponse>> FailedInFirstMessageError(ErrorCode errorCode, TimeSpan delay)
        {
            return async context => {
                if (context.CorrelationId == 1)
                {
                    await Task.Delay(delay);
                    await Task.Delay(1);
                    return new FetchResponse(new[] { new FetchResponse.Topic("foo", 1, 0, errorCode) });
                }
                return new FetchResponse();
            };
        }

        private Func<IRequestContext, Task<IResponse>> FailedInFirstMessageException(Type exceptionType, TimeSpan delay)
        {
            return async context =>
            {
                if (context.CorrelationId == 1)
                {
                    await Task.Delay(delay.Add(TimeSpan.FromMilliseconds(1)));
                    if (exceptionType == typeof(ConnectionException)) {
                        throw new ConnectionException("error test");
                    }
                    if (exceptionType == typeof (RequestException)) {
                        throw new RequestException(ApiKey.CreateTopics, ErrorCode.BROKER_NOT_AVAILABLE, TestConfig.ServerEndpoint());
                    }
                    if (exceptionType == typeof (FetchOutOfRangeException)) {
                        throw new FetchOutOfRangeException(new FetchRequest.Topic("name", 0, 0L), ErrorCode.BROKER_NOT_AVAILABLE, TestConfig.ServerEndpoint());
                    }
                    var args = new object[] { "error Test" };
                    throw (Exception)Activator.CreateInstance(exceptionType, args);
                }
                return new FetchResponse();
            };
        }

        private void CreateSuccessfulSendMock(RoutingScenario router)
        {
            router.Connection1.Add(ApiKey.Fetch, ShouldReturnValidMessage);
            router.Connection1.Add(ApiKey.Metadata, async _ => await RoutingScenario.DefaultMetadataResponse());
            router.Connection2.Add(ApiKey.Fetch, ShouldReturnValidMessage);
            router.Connection2.Add(ApiKey.Metadata, async _ => await RoutingScenario.DefaultMetadataResponse());
        }

        private Task<IResponse> ShouldReturnValidMessage(IRequestContext context)
        {
            return Task.FromResult((IResponse)new FetchResponse());
        }

        #endregion

        #region Construction

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

        #endregion

        #region Connection

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
                    .Returns(_ => RoutingScenario.DefaultMetadataResponse());
            }
            var factory = CreateFactory(connections);
            var router = new Router(new Endpoint(new IPEndPoint(IPAddress.Loopback, 1)), factory);

            // Act
            var testTopic = RoutingScenario.TestTopic;
            await router.GetTopicMetadataAsync(testTopic, CancellationToken.None);
            var topics = router.GetTopicMetadata(testTopic);

            // Assert
            factory.Received()
                   .Create(Arg.Is<Endpoint>(e => e.Ip.Port == 2), Arg.Any<IConnectionConfiguration>(), Arg.Any<ILog>());
        }

        [Test]
        public async Task BrokerRouterUsesFactoryToAddNewBrokersFromGroups()
        {
            // Arrange
            var connections = CreateConnections(2);
            foreach (var connection in connections) {
                connection
                    .SendAsync(Arg.Any<GroupCoordinatorRequest>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                    .Returns(_ => RoutingScenario.DefaultGroupCoordinatorResponse(1));
            }
            var factory = CreateFactory(connections);
            var router = new Router(new Endpoint(new IPEndPoint(IPAddress.Loopback, 1)), factory);

            // Act
            var testTopic = RoutingScenario.TestTopic;
            await router.GetGroupConnectionAsync(testTopic, CancellationToken.None);
            var broker = router.GetGroupConnection(testTopic);

            // Assert
            factory.Received()
                   .Create(Arg.Is<Endpoint>(e => e.Ip.Port == 2), Arg.Any<IConnectionConfiguration>(), Arg.Any<ILog>());
        }

        #endregion

        #region Group

        [Test]
        public async Task GetGroupShouldThrowWhenBrokerCollectionIsEmpty()
        {
            var scenario = new RoutingScenario();
            var router = scenario.CreateRouter();
            Assert.Throws<RoutingException>(() => router.GetGroupConnection("unknown"));
        }

        [Test]
        public async Task BrokerRouteShouldCycleThroughEachBrokerUntilOneIsFoundForGroup()
        {
            var scenario = new RoutingScenario();
            scenario.Connection1.Add(ApiKey.GroupCoordinator, _ => { throw new Exception("some error"); });
            var router = scenario.CreateRouter();
            var testTopic = RoutingScenario.TestTopic;
            await router.GetGroupConnectionAsync(testTopic, CancellationToken.None);
            var result = router.GetGroupConnection(testTopic);
            Assert.That(result, Is.Not.Null);
            Assert.That(scenario.Connection1[ApiKey.GroupCoordinator], Is.EqualTo(1));
            Assert.That(scenario.Connection2[ApiKey.GroupCoordinator], Is.EqualTo(1));
        }

        [Test]
        public async Task BrokerRouteShouldThrowIfCycleCouldNotConnectToAnyServerForGroup()
        {
            var scenario = new RoutingScenario();
            scenario.Connection1.Add(ApiKey.GroupCoordinator, _ => { throw new Exception("some error"); });
            scenario.Connection2.Add(ApiKey.GroupCoordinator, _ => { throw new Exception("some error"); });
            var router = scenario.CreateRouter();

            Assert.ThrowsAsync<RoutingException>(async () => await router.GetGroupConnectionAsync(RoutingScenario.TestTopic, CancellationToken.None));

            Assert.That(scenario.Connection1[ApiKey.GroupCoordinator], Is.EqualTo(1));
            Assert.That(scenario.Connection2[ApiKey.GroupCoordinator], Is.EqualTo(1));
        }

        [Test]
        public async Task BrokerRouteShouldReturnGroupFromCache()
        {
            var scenario = new RoutingScenario();
            var router = scenario.CreateRouter();
            var testTopic = RoutingScenario.TestTopic;
            await router.GetGroupConnectionAsync(testTopic, CancellationToken.None);
            var result1 = router.GetGroupConnection(testTopic);
            var result2 = router.GetGroupConnection(testTopic);

            Assert.That(scenario.Connection1[ApiKey.GroupCoordinator], Is.EqualTo(1));
            Assert.That(result1.GroupId, Is.EqualTo(testTopic));
            Assert.That(result2.GroupId, Is.EqualTo(testTopic));
        }

        [Test]
        public async Task RefreshGroupMetadataShouldIgnoreCacheAndAlwaysCauseRequestAfterExpirationDate()
        {
            var scenario = new RoutingScenario();
            var cacheExpiration = TimeSpan.FromMilliseconds(100);
            var router = scenario.CreateRouter(cacheExpiration);
            var testTopic = RoutingScenario.TestTopic;
            await router.RefreshGroupConnectionAsync(testTopic, true, CancellationToken.None);
            Assert.That(scenario.Connection1[ApiKey.GroupCoordinator], Is.EqualTo(1));
            await Task.Delay(cacheExpiration.Add(TimeSpan.FromMilliseconds(1))); // After cache is expired
            await router.RefreshGroupConnectionAsync(testTopic, true, CancellationToken.None);
            Assert.That(scenario.Connection1[ApiKey.GroupCoordinator], Is.EqualTo(2));
        }

        [Test]
        public async Task SimultaneouslyRefreshGroupMetadataShouldNotGetDataFromCacheOnSameRequest()
        {
            var scenario = new RoutingScenario();
            var router = scenario.CreateRouter();

            var testTopic = RoutingScenario.TestTopic;
            await Task.WhenAll(
                router.RefreshGroupConnectionAsync(testTopic, true, CancellationToken.None),
                router.RefreshGroupConnectionAsync(testTopic, true, CancellationToken.None));
            Assert.That(scenario.Connection1[ApiKey.GroupCoordinator], Is.EqualTo(2));
        }

        [Test]
        public async Task SimultaneouslyGetGroupMetadataShouldGetDataFromCacheOnSameRequest()
        {
            var scenario = new RoutingScenario();
            var router = scenario.CreateRouter(TimeSpan.FromMinutes(1)); // long timeout to avoid race condition on lock lasting longer than cache timeout

            var testTopic = RoutingScenario.TestTopic;
            await Task.WhenAll(
                router.GetGroupConnectionAsync(testTopic, CancellationToken.None), 
                router.GetGroupConnectionAsync(testTopic, CancellationToken.None));
            Assert.That(scenario.Connection1[ApiKey.GroupCoordinator], Is.EqualTo(1));
        }

        #endregion

        #region Topic Metadata

        [Test]
        [TestCase(ErrorCode.LEADER_NOT_AVAILABLE)]
        [TestCase(ErrorCode.GROUP_LOAD_IN_PROGRESS)]
        [TestCase(ErrorCode.GROUP_COORDINATOR_NOT_AVAILABLE)]
        public async Task ShouldRetryWhenReceiveAnRetryErrorCode(ErrorCode errorCode)
        {
            var conn = Substitute.For<IConnection>();

            conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>())
                .Returns(x => CreateMetadataResponse(errorCode), x => CreateMetadataResponse(errorCode));

            var router = GetRouter(conn);
            var log = new MemoryLog();
            router.Log.ReturnsForAnyArgs(log);
            var response = await router.GetMetadataAsync(new MetadataRequest("Test"), CancellationToken.None);

            Received.InOrder(() =>
            {
                conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>());
                //_log.OnLogged(LogLevel.Warn, It.Is<LogEvent>(e => e.Message.StartsWith("Failed metadata request on attempt 0: Will retry in")));
                conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>());
                //_log.OnLogged(LogLevel.Warn, It.Is<LogEvent>(e => e.Message.StartsWith("Failed metadata request on attempt 1: Will retry in")));
                conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>());
            });

            Assert.That(log.LogEvents.Any(e => e.Item1 == LogLevel.Warn && e.Item2.Message.StartsWith("Failed metadata request on attempt 0: Will retry in 00:00:00")));
            Assert.That(log.LogEvents.Any(e => e.Item1 == LogLevel.Warn && e.Item2.Message.StartsWith("Failed metadata request on attempt 1: Will retry in 00:00:00")));
            Assert.That(log.LogEvents.Any(e => e.Item1 == LogLevel.Warn && e.Item2.Message.StartsWith("Failed metadata request on attempt 2: Will retry in 00:00:00")), Is.False);
            Assert.That(log.LogEvents.Count(e => e.Item1 == LogLevel.Warn && e.Item2.Message.StartsWith("Failed metadata request on attempt")), Is.EqualTo(3));
        }

        private static IRouter GetRouter(IConnection conn)
        {
            var router = Substitute.For<IRouter>();
            router.Configuration.ReturnsForAnyArgs(TestConfig.Options.RouterConfiguration);
            router.Connections.ReturnsForAnyArgs(new List<IConnection> { conn });
            return router;
        }

        [Test]
        public async Task ShouldRetryWhenReceiveBrokerIdNegativeOne()
        {
            var conn = Substitute.For<IConnection>();

            conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                .Returns(x => CreateMetadataResponse(-1, "123", 1), x => CreateMetadataResponse(ErrorCode.NONE));

            var router = GetRouter(conn);
            var log = new MemoryLog();
            router.Log.ReturnsForAnyArgs(log);
            var response = await router.GetMetadataAsync(new MetadataRequest("Test"), CancellationToken.None);

            Received.InOrder(() =>
            {
                conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>());
                //_log.OnLogged.Invoke(LogLevel.Warn, It.Is<LogEvent>(e => e.Message.StartsWith("Failed metadata request on attempt 0: Will retry in")));
                conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>());
            });

            Assert.That(log.LogEvents.Any(e => e.Item1 == LogLevel.Warn && e.Item2.Message.StartsWith("Failed metadata request on attempt 0: Will retry in")));
            Assert.That(log.LogEvents.Count(e => e.Item1 == LogLevel.Warn && e.Item2.Message.StartsWith("Failed metadata request on attempt")), Is.EqualTo(1));
        }

        [Test]
        public void ShouldReturnWhenNoErrorReceived()
        {
            var conn = Substitute.For<IConnection>();

            conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>())
                .Returns(x => CreateMetadataResponse(ErrorCode.NONE));

            var router = GetRouter(conn);
            var source = new CancellationTokenSource();
            var response = router.GetMetadataAsync(new MetadataRequest("Test"), source.Token);
            source.Cancel();

            conn.Received(1).SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>());
        }

        [Test]
        public void ShouldReturnWhenNoErrorReceivedAndTopicsNotSpecified()
        {
            var conn = Substitute.For<IConnection>();

            conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>())
                .Returns(x => CreateMetadataResponse(ErrorCode.NONE));

            var router = GetRouter(conn);
            var source = new CancellationTokenSource();
            var response = router.GetMetadataAsync(new MetadataRequest(), source.Token);
            source.Cancel();

            conn.Received(1).SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>());
        }

        [Test]
        [TestCase(ErrorCode.UNKNOWN)]
        [TestCase(ErrorCode.INVALID_TOPIC_EXCEPTION)]
        [TestCase(ErrorCode.INVALID_REQUIRED_ACKS)]
        public async Task ShouldThrowExceptionWhenNotARetriableErrorCode(ErrorCode errorCode)
        {
            var conn = Substitute.For<IConnection>();

            conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>()).Returns(x => CreateMetadataResponse(errorCode));

            var router = GetRouter(conn);
            Assert.ThrowsAsync<RequestException>(() => router.GetMetadataAsync(new MetadataRequest("Test"), CancellationToken.None));
        }

        [Test]
        [TestCase(null)]
        [TestCase("")]
        public void ShouldThrowExceptionWhenHostIsMissing(string host)
        {
            var conn = Substitute.For<IConnection>();

            conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>()).Returns(x => CreateMetadataResponse(1, host, 1));

            var router = GetRouter(conn);
            Assert.ThrowsAsync<ConnectionException>(() => router.GetMetadataAsync(new MetadataRequest("Test"), CancellationToken.None));
        }

        [Test]
        [TestCase(0)]
        [TestCase(-1)]
        public void ShouldThrowExceptionWhenPortIsMissing(int port)
        {
            var conn = Substitute.For<IConnection>();

            conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>()).Returns(x => CreateMetadataResponse(1, "123", port));

            var router = GetRouter(conn);
            Assert.ThrowsAsync<ConnectionException>(() => router.GetMetadataAsync(new MetadataRequest("Test"), CancellationToken.None));
        }

#pragma warning disable 1998
        private Task<MetadataResponse> CreateMetadataResponse(int brokerId, string host, int port)
        {
            var tcs = new TaskCompletionSource<MetadataResponse>();
            tcs.SetResult(new MetadataResponse(new[] { new KafkaClient.Protocol.Server(brokerId, host, port) }, new MetadataResponse.Topic[] { }));
            return tcs.Task;
        }

        private async Task<MetadataResponse> CreateMetadataResponse(ErrorCode errorCode)
        {
            return new MetadataResponse(new KafkaClient.Protocol.Server[] { }, new[] { new MetadataResponse.Topic("Test", errorCode, new MetadataResponse.Partition[] { }) });
        }
#pragma warning restore 1998

        [Test]
        public async Task BrokerRouteShouldCycleThroughEachBrokerUntilOneIsFound()
        {
            var scenario = new RoutingScenario();
            scenario.Connection1.Add(ApiKey.Metadata, _ => { throw new Exception("some error"); });
            var router = scenario.CreateRouter();
            var testTopic = RoutingScenario.TestTopic;
            await router.GetTopicMetadataAsync(testTopic, CancellationToken.None);
            var result = router.GetTopicMetadata(testTopic);
            Assert.That(result, Is.Not.Null);
            Assert.That(scenario.Connection1[ApiKey.Metadata], Is.EqualTo(1));
            Assert.That(scenario.Connection2[ApiKey.Metadata], Is.EqualTo(1));
        }

        [Test]
        public async Task BrokerRouteShouldThrowIfCycleCouldNotConnectToAnyServer()
        {
            var scenario = new RoutingScenario();
            scenario.Connection1.Add(ApiKey.Metadata, _ => { throw new ConnectionException("some error"); });
            scenario.Connection2.Add(ApiKey.Metadata, _ => { throw new ConnectionException("some error"); });
            var router = scenario.CreateRouter();

            await AssertAsync.Throws<ConnectionException>(() => router.GetTopicMetadataAsync(RoutingScenario.TestTopic, CancellationToken.None));

            // 3 attempts total, round robin so 2 to connection1, 1 to connection2
            Assert.That(scenario.Connection1[ApiKey.Metadata], Is.EqualTo(2));
            Assert.That(scenario.Connection2[ApiKey.Metadata], Is.EqualTo(1));
        }

        [Test]
        public async Task GetTopicShouldReturnTopic()
        {
            var scenario = new RoutingScenario();
            var router = scenario.CreateRouter();
            await router.GetTopicMetadataAsync(RoutingScenario.TestTopic, CancellationToken.None);

            var result = router.GetTopicMetadata(RoutingScenario.TestTopic);
            Assert.That(result.TopicName, Is.EqualTo(RoutingScenario.TestTopic));
        }

        [Test]
        public void EmptyTopicMetadataShouldThrowException()
        {
            var scenario = new RoutingScenario();
            var router = scenario.CreateRouter();

            Assert.Throws<RoutingException>(() => router.GetTopicMetadata("MissingTopic"));
        }

        [Test]
        public async Task BrokerRouteShouldReturnTopicFromCache()
        {
            var scenario = new RoutingScenario();
            var router = scenario.CreateRouter();
            var testTopic = RoutingScenario.TestTopic;
            await router.GetTopicMetadataAsync(testTopic, CancellationToken.None);
            var result1 = router.GetTopicMetadata(testTopic);
            var result2 = router.GetTopicMetadata(testTopic);

            Assert.AreEqual(1, router.GetTopicMetadata().Count);
            Assert.That(scenario.Connection1[ApiKey.Metadata], Is.EqualTo(1));
            Assert.That(result1.TopicName, Is.EqualTo(testTopic));
            Assert.That(result2.TopicName, Is.EqualTo(testTopic));
        }

        [Test]
        public async Task BrokerRouteShouldThrowNoLeaderElectedForPartition()
        {
            var scenario = new RoutingScenario {
                MetadataResponse = RoutingScenario.MetadataResponseWithNotEndToElectLeader
            };

            var router = scenario.CreateRouter();
            await AssertAsync.Throws<RoutingException>(() => router.GetTopicMetadataAsync(RoutingScenario.TestTopic, CancellationToken.None));
            Assert.AreEqual(0, router.GetTopicMetadata().Count);
        }

        [Test]
        public async Task BrokerRouteShouldReturnAllTopicsFromCache()
        {
            var scenario = new RoutingScenario();
            var router = scenario.CreateRouter();
            await router.RefreshTopicMetadataAsync(CancellationToken.None);
            var result1 = router.GetTopicMetadata();
            var result2 = router.GetTopicMetadata();

            Assert.That(scenario.Connection1[ApiKey.Metadata], Is.EqualTo(1));
            Assert.That(result1.Count, Is.EqualTo(1));
            var testTopic = RoutingScenario.TestTopic;
            Assert.That(result1[0].TopicName, Is.EqualTo(testTopic));
            Assert.That(result2.Count, Is.EqualTo(1));
            Assert.That(result2[0].TopicName, Is.EqualTo(testTopic));
        }

        [Test]
        public async Task RefreshTopicMetadataShouldIgnoreCacheAndAlwaysCauseMetadataRequestAfterExpirationDate()
        {
            var scenario = new RoutingScenario();
            var cacheExpiration = TimeSpan.FromMilliseconds(100);
            var router = scenario.CreateRouter(cacheExpiration);
            var testTopic = RoutingScenario.TestTopic;
            await router.RefreshTopicMetadataAsync(testTopic, true, CancellationToken.None);
            Assert.That(scenario.Connection1[ApiKey.Metadata], Is.EqualTo(1));
            await Task.Delay(cacheExpiration.Add(TimeSpan.FromMilliseconds(1))); // After cache is expired
            await router.RefreshTopicMetadataAsync(testTopic, true, CancellationToken.None);
            Assert.That(scenario.Connection1[ApiKey.Metadata], Is.EqualTo(2));
        }

        [Test]
        public async Task RefreshAllTopicMetadataShouldAlwaysDoRequest()
        {
            var scenario = new RoutingScenario();
            var router = scenario.CreateRouter();
            await router.RefreshTopicMetadataAsync(CancellationToken.None);
            Assert.That(scenario.Connection1[ApiKey.Metadata], Is.EqualTo(1));
            await router.RefreshTopicMetadataAsync(CancellationToken.None);
            Assert.That(scenario.Connection1[ApiKey.Metadata], Is.EqualTo(2));
        }

        [Test]
        public async Task SelectBrokerRouteShouldChange()
        {
            var scenario = new RoutingScenario();

            var cacheExpiry = TimeSpan.FromMilliseconds(1);
            var router = scenario.CreateRouter(cacheExpiry);

            scenario.MetadataResponse = RoutingScenario.DefaultMetadataResponse;
            var testTopic = RoutingScenario.TestTopic;
            await router.RefreshTopicMetadataAsync(testTopic, true, CancellationToken.None);

            var router1 = router.GetTopicConnection(testTopic, 0);

            Assert.That(scenario.Connection1[ApiKey.Metadata], Is.EqualTo(1));
            await Task.Delay(cacheExpiry.Add(TimeSpan.FromMilliseconds(1))); // After cache is expired
            scenario.MetadataResponse = RoutingScenario.MetadataResponseWithSingleBroker;
            await router.RefreshTopicMetadataAsync(testTopic, true, CancellationToken.None);
            var router2 = router.GetTopicConnection(testTopic, 0);

            Assert.That(scenario.Connection1[ApiKey.Metadata], Is.EqualTo(2));
            Assert.That(router1.Connection.Endpoint, Is.EqualTo(scenario.Connection1.Endpoint));
            Assert.That(router2.Connection.Endpoint, Is.EqualTo(scenario.Connection2.Endpoint));
            Assert.That(router1.Connection.Endpoint, Is.Not.EqualTo(router2.Connection.Endpoint));
        }

        [Test]
        public async Task SimultaneouslyRefreshTopicMetadataShouldNotGetDataFromCacheOnSameRequest()
        {
            var scenario = new RoutingScenario();
            var router = scenario.CreateRouter();

            var testTopic = RoutingScenario.TestTopic;
            await Task.WhenAll(
                router.RefreshTopicMetadataAsync(testTopic, true, CancellationToken.None), 
                router.RefreshTopicMetadataAsync(testTopic, true, CancellationToken.None)
                ); //do not debug
            Assert.That(scenario.Connection1[ApiKey.Metadata], Is.EqualTo(2));
        }

        [Test]
        public async Task SimultaneouslyGetTopicMetadataShouldGetDataFromCacheOnSameRequest()
        {
            var scenario = new RoutingScenario();
            var router = scenario.CreateRouter(TimeSpan.FromMinutes(1)); // long timeout to avoid race condition on lock lasting longer than cache timeout

            var testTopic = RoutingScenario.TestTopic;
            await Task.WhenAll(
                router.GetTopicMetadataAsync(testTopic, CancellationToken.None), 
                router.GetTopicMetadataAsync(testTopic, CancellationToken.None)
                );
            Assert.That(scenario.Connection1[ApiKey.Metadata], Is.EqualTo(1));
        }

        #endregion

        #region SelectBrokerRouteAsync

        [Test]
        public async Task SelectExactPartitionShouldReturnRequestedPartition()
        {
            var scenario = new RoutingScenario();
            var router = scenario.CreateRouter();
            var testTopic = RoutingScenario.TestTopic;
            await router.GetTopicMetadataAsync(testTopic, CancellationToken.None);
            var p0 = router.GetTopicConnection(testTopic, 0);
            var p1 = router.GetTopicConnection(testTopic, 1);

            Assert.That(p0.PartitionId, Is.EqualTo(0));
            Assert.That(p1.PartitionId, Is.EqualTo(1));
        }

        [Test]
        public async Task SelectExactPartitionShouldThrowWhenPartitionDoesNotExist()
        {
            var scenario = new RoutingScenario();
            var router = scenario.CreateRouter();
            var testTopic = RoutingScenario.TestTopic;
            await router.GetTopicMetadataAsync(testTopic, CancellationToken.None);
            Assert.Throws<RoutingException>(() => router.GetTopicConnection(testTopic, 3));
        }

        [Test]
        public async Task SelectExactPartitionShouldThrowWhenTopicsCollectionIsEmpty()
        {
            var metadataResponse = await RoutingScenario.DefaultMetadataResponse();
            metadataResponse.Topics.Clear();

            var scenario = new RoutingScenario();
#pragma warning disable 1998
            scenario.Connection1.Add(ApiKey.Metadata, async _ => metadataResponse);
#pragma warning restore 1998

            Assert.Throws<RoutingException>(() => scenario.CreateRouter().GetTopicConnection(RoutingScenario.TestTopic, 1));
        }

        [Test]
        public async Task SelectExactPartitionShouldThrowWhenBrokerCollectionIsEmpty()
        {
            var metadataResponse = await RoutingScenario.DefaultMetadataResponse();
            metadataResponse = new MetadataResponse(topics: metadataResponse.Topics);

            var scenario = new RoutingScenario();
#pragma warning disable 1998
            scenario.Connection1.Add(ApiKey.Metadata, async _ => metadataResponse);
#pragma warning restore 1998
            var router = scenario.CreateRouter();
            var testTopic = RoutingScenario.TestTopic;
            await router.GetTopicMetadataAsync(testTopic, CancellationToken.None);
            Assert.Throws<RoutingException>(() => router.GetTopicConnection(testTopic, 1));
        }

        #endregion

        #region GetTopicOffset

        [Test]
        public async Task GetTopicOffsetShouldQueryEachBroker()
        {
            var scenario = new RoutingScenario();
            var router = scenario.CreateRouter();

            await router.GetTopicOffsetsAsync(RoutingScenario.TestTopic, 2, -1, CancellationToken.None);
            Assert.That(scenario.Connection1[ApiKey.Offsets], Is.EqualTo(1));
            Assert.That(scenario.Connection2[ApiKey.Offsets], Is.EqualTo(1));
        }

        [Test]
        public async Task GetTopicOffsetShouldThrowAnyException()
        {
            var scenario = new RoutingScenario();
            scenario.Connection1.Add(ApiKey.Offsets, _ => { throw new BufferUnderRunException("test 99"); });
            var router = scenario.CreateRouter();

            await AssertAsync.Throws<BufferUnderRunException>(
                () => router.GetTopicOffsetsAsync(RoutingScenario.TestTopic, 2, -1, CancellationToken.None),
                ex => ex.Message.Contains("test 99"));
        }

        #endregion
    }
}