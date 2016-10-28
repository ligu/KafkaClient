using System;
using System.Linq;
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
#pragma warning disable 1998

namespace KafkaClient.Tests
{
    [TestFixture]
    [Category("Unit")]
    public class RouterBrokerSendAsyncTests
    {
        private MoqMockingKernel _kernel;
        private Mock<IConnection> _mockKafkaConnection1;
        private Mock<IConnectionFactory> _mockKafkaConnectionFactory;
        private int _partitionId = 0;

        [SetUp]
        public void Setup()
        {
            _kernel = new MoqMockingKernel();
            _mockKafkaConnection1 = _kernel.GetMock<IConnection>();
            _mockKafkaConnectionFactory = _kernel.GetMock<IConnectionFactory>();
            _mockKafkaConnectionFactory.Setup(x => x.Create(It.Is<Endpoint>(e => e.IP.Port == 1), It.IsAny<IConnectionConfiguration>(), It.IsAny<ILog>())).Returns(() => _mockKafkaConnection1.Object);
            _mockKafkaConnectionFactory.Setup(x => x.Resolve(It.IsAny<Uri>(), It.IsAny<ILog>()))
                .Returns<Uri, ILog>((uri, log) => new Endpoint(uri, new IPEndPoint(IPAddress.Parse("127.0.0.1"), uri.Port)));
        }

        [TestCase(ErrorResponseCode.NotLeaderForPartition)]
        [TestCase(ErrorResponseCode.LeaderNotAvailable)]
        [TestCase(ErrorResponseCode.GroupCoordinatorNotAvailable)]
        [TestCase(ErrorResponseCode.UnknownTopicOrPartition)]
        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public async Task ShouldTryToRefreshMataDataIfCanRecoverByRefreshMetadata(ErrorResponseCode code)
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            routerProxy._cacheExpiration = new TimeSpan(10);
            var router = routerProxy.Create();

            routerProxy.BrokerConn0.FetchResponseFunction = FailedInFirstMessageError(code, routerProxy._cacheExpiration);
            routerProxy.BrokerConn0.MetadataResponseFunction = BrokerRouterProxy.CreateMetadataResponseWithMultipleBrokers;

            await router.SendAsync(new FetchRequest(), BrokerRouterProxy.TestTopic, _partitionId, CancellationToken.None);

            Assert.That(routerProxy.BrokerConn0.MetadataRequestCallCount, Is.EqualTo(2));
            Assert.That(routerProxy.BrokerConn0.FetchRequestCallCount, Is.EqualTo(2));
        }

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        [TestCase(typeof(ConnectionException))]
        [TestCase(typeof(FetchOutOfRangeException))]
        [TestCase(typeof(CachedMetadataException))]
        public async Task ShouldTryToRefreshMataDataIfOnExceptions(Type exceptionType)
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            routerProxy._cacheExpiration = TimeSpan.FromMilliseconds(10);
            var router = routerProxy.Create();

            routerProxy.BrokerConn0.FetchResponseFunction = FailedInFirstMessageException(exceptionType, routerProxy._cacheExpiration);
            routerProxy.BrokerConn0.MetadataResponseFunction = BrokerRouterProxy.CreateMetadataResponseWithMultipleBrokers;

            await router.SendAsync(new FetchRequest(), BrokerRouterProxy.TestTopic, _partitionId, CancellationToken.None);

            Assert.That(routerProxy.BrokerConn0.MetadataRequestCallCount, Is.EqualTo(2));
            Assert.That(routerProxy.BrokerConn0.FetchRequestCallCount, Is.EqualTo(2));
        }

        [TestCase(typeof(Exception))]
        [TestCase(typeof(RequestException))]
        public async Task SendProtocolRequestShouldThrowException(Type exceptionType)
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            routerProxy._cacheExpiration = TimeSpan.FromMilliseconds(10);
            var router = routerProxy.Create();

            routerProxy.BrokerConn0.FetchResponseFunction = FailedInFirstMessageException(exceptionType, routerProxy._cacheExpiration);
            routerProxy.BrokerConn0.MetadataResponseFunction = BrokerRouterProxy.CreateMetadataResponseWithMultipleBrokers;
            try
            {
                await router.SendAsync(new FetchRequest(), BrokerRouterProxy.TestTopic, _partitionId, CancellationToken.None);
                Assert.IsTrue(false, "Should throw exception");
            }
            catch (Exception ex)
            {
                Assert.That(ex.GetType(), Is.EqualTo(exceptionType));
            }
        }

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        [TestCase(ErrorResponseCode.InvalidFetchSize)]
        [TestCase(ErrorResponseCode.MessageTooLarge)]
        [TestCase(ErrorResponseCode.OffsetMetadataTooLarge)]
        [TestCase(ErrorResponseCode.OffsetOutOfRange)]
        [TestCase(ErrorResponseCode.Unknown)]
        [TestCase(ErrorResponseCode.StaleControllerEpoch)]
        [TestCase(ErrorResponseCode.ReplicaNotAvailable)]
        public async Task SendProtocolRequestShouldNotTryToRefreshMataDataIfCanNotRecoverByRefreshMetadata(
            ErrorResponseCode code)
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            routerProxy._cacheExpiration = TimeSpan.FromMilliseconds(10);
            var router = routerProxy.Create();

            routerProxy.BrokerConn0.FetchResponseFunction = FailedInFirstMessageError(code, routerProxy._cacheExpiration);
            routerProxy.BrokerConn0.MetadataResponseFunction = BrokerRouterProxy.CreateMetadataResponseWithMultipleBrokers;
            Assert.ThrowsAsync<RequestException>(async () => await router.SendAsync(new FetchRequest(), BrokerRouterProxy.TestTopic, _partitionId, CancellationToken.None));
        }

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public async Task ShouldUpdateMetadataOnce()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            routerProxy._cacheExpiration = TimeSpan.FromMilliseconds(10);
            var router = routerProxy.Create();

            routerProxy.BrokerConn0.FetchResponseFunction = ShouldReturnValidMessage;
            routerProxy.BrokerConn0.MetadataResponseFunction = BrokerRouterProxy.CreateMetadataResponseWithMultipleBrokers;
            int numberOfCall = 1000;
            Task[] tasks = new Task[numberOfCall];
            for (int i = 0; i < numberOfCall / 2; i++)
            {
                tasks[i] = router.SendAsync(new FetchRequest(), BrokerRouterProxy.TestTopic, _partitionId, CancellationToken.None);
            }
            await Task.Delay(routerProxy._cacheExpiration);
            await Task.Delay(1);
            for (int i = 0; i < numberOfCall / 2; i++)
            {
                tasks[i + numberOfCall / 2] = router.SendAsync(new FetchRequest(), BrokerRouterProxy.TestTopic, _partitionId, CancellationToken.None);
            }

            await Task.WhenAll(tasks);
            Assert.That(routerProxy.BrokerConn0.FetchRequestCallCount, Is.EqualTo(numberOfCall));
            Assert.That(routerProxy.BrokerConn0.MetadataRequestCallCount, Is.EqualTo(1));
        }

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public async Task ShouldRecoverUpdateMetadataForNewTopic()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            routerProxy._cacheExpiration = TimeSpan.FromMilliseconds(10);
            var router = routerProxy.Create();

            var fetchRequest = new FetchRequest();

            routerProxy.BrokerConn0.FetchResponseFunction = ShouldReturnValidMessage;
            routerProxy.BrokerConn0.MetadataResponseFunction = BrokerRouterProxy.CreateMetadataResponseWithMultipleBrokers;
            int numberOfCall = 1000;
            Task[] tasks = new Task[numberOfCall];
            for (int i = 0; i < numberOfCall / 2; i++)
            {
                tasks[i] = router.SendAsync(fetchRequest, BrokerRouterProxy.TestTopic, _partitionId, CancellationToken.None);
            }

            routerProxy.BrokerConn0.MetadataResponseFunction = async () =>
            {
                var response = await BrokerRouterProxy.CreateMetadataResponseWithMultipleBrokers();
                return new MetadataResponse(response.Brokers, response.Topics.Select(t => new MetadataTopic("test2", t.ErrorCode, t.Partitions)));
            };

            for (int i = 0; i < numberOfCall / 2; i++)
            {
                tasks[i + numberOfCall / 2] = router.SendAsync(fetchRequest, "test2", _partitionId, CancellationToken.None);
            }

            await Task.WhenAll(tasks);
            Assert.That(routerProxy.BrokerConn0.FetchRequestCallCount, Is.EqualTo(numberOfCall));
            Assert.That(routerProxy.BrokerConn0.MetadataRequestCallCount, Is.EqualTo(2));
        }

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public async Task ShouldRecoverFromFailureByUpdateMetadataOnce() //Do not debug this test !!
        {
            var log = new ConsoleLog();
            var routerProxy = new BrokerRouterProxy(_kernel);
            routerProxy._cacheExpiration = TimeSpan.FromMilliseconds(1000);
            var router = routerProxy.Create();

            int partitionId = 0;
            var fetchRequest = new FetchRequest();

            int numberOfCall = 100;
            long numberOfErrorSend = 0;
            TaskCompletionSource<int> x = new TaskCompletionSource<int>();
            Func<Task<FetchResponse>> ShouldReturnNotLeaderForPartitionAndThenNoError = async () =>
            {
                log.Debug(() => LogEvent.Create("FetchResponse Start "));
                if (!x.Task.IsCompleted)
                {
                    if (Interlocked.Increment(ref numberOfErrorSend) == numberOfCall)
                    {
                        await Task.Delay(routerProxy._cacheExpiration);
                        await Task.Delay(1);
                        x.TrySetResult(1);
                        log.Debug(() => LogEvent.Create("all is complete "));
                    }

                    await x.Task;
                    log.Debug(() => LogEvent.Create("SocketException "));
                    throw new ConnectionException("");
                }
                log.Debug(() => LogEvent.Create("Completed "));

                return new FetchResponse();
            };

            routerProxy.BrokerConn0.FetchResponseFunction = ShouldReturnNotLeaderForPartitionAndThenNoError;
            routerProxy.BrokerConn0.MetadataResponseFunction = BrokerRouterProxy.CreateMetadataResponseWithMultipleBrokers;

            Task[] tasks = new Task[numberOfCall];

            for (int i = 0; i < numberOfCall; i++)
            {
                tasks[i] = router.SendAsync(fetchRequest, BrokerRouterProxy.TestTopic, partitionId, CancellationToken.None);
            }

            await Task.WhenAll(tasks);
            Assert.That(numberOfErrorSend, Is.GreaterThan(1), "numberOfErrorSend");
            Assert.That(routerProxy.BrokerConn0.FetchRequestCallCount, Is.EqualTo(numberOfCall + numberOfErrorSend),
                "FetchRequestCallCount");
            Assert.That(routerProxy.BrokerConn0.MetadataRequestCallCount, Is.EqualTo(2), "MetadataRequestCallCount");
        }

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public async Task ShouldRecoverFromConnectionExceptionByUpdateMetadataOnceFullScenario() //Do not debug this test !!
        {
            await ShouldRecoverByUpdateMetadataOnceFullScenario(
                FailedInFirstMessageException(typeof(ConnectionException), TimeSpan.Zero));
        }

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public async Task ShouldRecoverFromFetchErrorByUpdateMetadataOnceFullScenario1()
        {
            await ShouldRecoverByUpdateMetadataOnceFullScenario(
                FailedInFirstMessageError(ErrorResponseCode.LeaderNotAvailable, TimeSpan.Zero));
        }

        /// <summary>
        /// Do not debug this test !!
        /// </summary>
        private async Task ShouldRecoverByUpdateMetadataOnceFullScenario(Func<Task<FetchResponse>> fetchResponse) 
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            routerProxy._cacheExpiration = TimeSpan.FromMilliseconds(0);
            var router = routerProxy.Create();
            int partitionId = 0;
            var fetchRequest = new FetchRequest();

            CreateSuccessfulSendMock(routerProxy);

            //Send Successful Message
            await router.SendAsync(fetchRequest, BrokerRouterProxy.TestTopic, partitionId, CancellationToken.None);

            Assert.That(routerProxy.BrokerConn0.FetchRequestCallCount, Is.EqualTo(1), "FetchRequestCallCount");
            Assert.That(routerProxy.BrokerConn0.MetadataRequestCallCount, Is.EqualTo(1), "MetadataRequestCallCount");
            Assert.That(routerProxy.BrokerConn1.MetadataRequestCallCount, Is.EqualTo(0), "MetadataRequestCallCount");

            routerProxy.BrokerConn0.FetchResponseFunction = fetchResponse;
            //triger to update metadata
            routerProxy.BrokerConn0.MetadataResponseFunction = BrokerRouterProxy.CreateMetaResponseWithException;
            routerProxy.BrokerConn1.MetadataResponseFunction = BrokerRouterProxy.CreateMetadataResponseWithSingleBroker;

            //Reset variables
            routerProxy.BrokerConn0.FetchRequestCallCount = 0;
            routerProxy.BrokerConn1.FetchRequestCallCount = 0;
            routerProxy.BrokerConn0.MetadataRequestCallCount = 0;
            routerProxy.BrokerConn1.MetadataRequestCallCount = 0;

            //Send Successful Message that was recover from exception
            await router.SendAsync(fetchRequest, BrokerRouterProxy.TestTopic, partitionId, CancellationToken.None);

            Assert.That(routerProxy.BrokerConn0.FetchRequestCallCount, Is.EqualTo(1), "FetchRequestCallCount");
            Assert.That(routerProxy.BrokerConn0.MetadataRequestCallCount, Is.EqualTo(1), "MetadataRequestCallCount");

            Assert.That(routerProxy.BrokerConn1.FetchRequestCallCount, Is.EqualTo(1), "FetchRequestCallCount");
            Assert.That(routerProxy.BrokerConn1.MetadataRequestCallCount, Is.EqualTo(1), "MetadataRequestCallCount");
        }


        private static Func<Task<FetchResponse>> FailedInFirstMessageError(ErrorResponseCode errorResponseCode, TimeSpan delay)
        {
            bool firstTime = true;
            Func<Task<FetchResponse>> result = async () => {
                if (firstTime) {
                    await Task.Delay(delay);
                    await Task.Delay(1);
                    firstTime = false;
                    return new FetchResponse(new []{ new FetchTopicResponse("foo", 1, 0, errorResponseCode)});
                }
                return new FetchResponse();
            };
            return result;
        }

        private Func<Task<FetchResponse>> FailedInFirstMessageException(Type exceptionType, TimeSpan delay)
        {
            bool firstTime = true;
            Func<Task<FetchResponse>> result = async () =>
            {
                if (firstTime)
                {
                    await Task.Delay(delay);
                    await Task.Delay(1);
                    firstTime = false;
                    if (exceptionType == typeof(ConnectionException))
                    {
                        throw new ConnectionException("");
                    }
                    object[] args = new object[1];
                    args[0] = "error Test";
                    throw (Exception)Activator.CreateInstance(exceptionType, args);
                }
                return new FetchResponse();
            };
            return result;
        }

        private void CreateSuccessfulSendMock(BrokerRouterProxy routerProxy)
        {
            routerProxy.BrokerConn0.FetchResponseFunction = ShouldReturnValidMessage;
            routerProxy.BrokerConn0.MetadataResponseFunction = BrokerRouterProxy.CreateMetadataResponseWithMultipleBrokers;
            routerProxy.BrokerConn1.FetchResponseFunction = ShouldReturnValidMessage;
            routerProxy.BrokerConn1.MetadataResponseFunction = BrokerRouterProxy.CreateMetadataResponseWithMultipleBrokers;
        }

        private Task<FetchResponse> ShouldReturnValidMessage()
        {
            return Task.FromResult(new FetchResponse());
        }
    }
}