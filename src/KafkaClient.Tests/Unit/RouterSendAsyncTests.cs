using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Connections;
using KafkaClient.Protocol;
using NUnit.Framework;

#pragma warning disable 1998

namespace KafkaClient.Tests.Unit
{
    [TestFixture]
    public class RouterSendAsyncTests
    {
        private const int PartitionId = 0;

        [TestCase(ErrorCode.NotLeaderForPartition)]
        [TestCase(ErrorCode.LeaderNotAvailable)]
        [TestCase(ErrorCode.GroupCoordinatorNotAvailable)]
        [TestCase(ErrorCode.UnknownTopicOrPartition)]
        [Test]
        public async Task ShouldTryToRefreshMataDataIfCanRecoverByRefreshMetadata(ErrorCode code)
        {
            var routerProxy = new FakeRouter();
            var cacheExpiration = new TimeSpan(10);
            var router = routerProxy.Create(cacheExpiration);

            routerProxy.Connection1.Add(ApiKey.Fetch, FailedInFirstMessageError(code, cacheExpiration));
            routerProxy.Connection1.Add(ApiKey.Metadata, async _ => await FakeRouter.DefaultMetadataResponse());

            await router.SendAsync(new FetchRequest(), FakeRouter.TestTopic, PartitionId, CancellationToken.None);

            Assert.That(routerProxy.Connection1[ApiKey.Metadata], Is.EqualTo(2));
            Assert.That(routerProxy.Connection1[ApiKey.Fetch], Is.EqualTo(2));
        }

        [Test]
        [TestCase(typeof(ConnectionException))]
        [TestCase(typeof(FetchOutOfRangeException))]
        [TestCase(typeof(CachedMetadataException))]
        public async Task ShouldTryToRefreshMataDataIfOnExceptions(Type exceptionType)
        {
            var routerProxy = new FakeRouter();
            var cacheExpiration = TimeSpan.FromMilliseconds(10);
            var router = routerProxy.Create(cacheExpiration);

            routerProxy.Connection1.Add(ApiKey.Fetch, FailedInFirstMessageException(exceptionType, cacheExpiration));
            routerProxy.Connection1.Add(ApiKey.Metadata, async _ => await FakeRouter.DefaultMetadataResponse());

            await router.SendAsync(new FetchRequest(), FakeRouter.TestTopic, PartitionId, CancellationToken.None);

            Assert.That(routerProxy.Connection1[ApiKey.Metadata], Is.EqualTo(2));
            Assert.That(routerProxy.Connection1[ApiKey.Fetch], Is.EqualTo(2));
        }

        [TestCase(typeof(Exception))]
        [TestCase(typeof(RequestException))]
        public async Task SendProtocolRequestShouldThrowException(Type exceptionType)
        {
            var routerProxy = new FakeRouter();
            var cacheExpiration = TimeSpan.FromMilliseconds(10);
            var router = routerProxy.Create(cacheExpiration);

            routerProxy.Connection1.Add(ApiKey.Fetch, FailedInFirstMessageException(exceptionType, cacheExpiration));
            routerProxy.Connection1.Add(ApiKey.Metadata, async _ => await FakeRouter.DefaultMetadataResponse());
            Assert.ThrowsAsync(exceptionType, async () => await router.SendAsync(new FetchRequest(), FakeRouter.TestTopic, PartitionId, CancellationToken.None));
        }

        [Test]
        [TestCase(ErrorCode.InvalidFetchSize)]
        [TestCase(ErrorCode.MessageTooLarge)]
        [TestCase(ErrorCode.OffsetMetadataTooLarge)]
        [TestCase(ErrorCode.OffsetOutOfRange)]
        [TestCase(ErrorCode.Unknown)]
        [TestCase(ErrorCode.StaleControllerEpoch)]
        [TestCase(ErrorCode.ReplicaNotAvailable)]
        public async Task SendProtocolRequestShouldNotTryToRefreshMataDataIfCanNotRecoverByRefreshMetadata(
            ErrorCode code)
        {
            var routerProxy = new FakeRouter();
            var cacheExpiration = TimeSpan.FromMilliseconds(10);
            var router = routerProxy.Create(cacheExpiration);

            routerProxy.Connection1.Add(ApiKey.Fetch, FailedInFirstMessageError(code, cacheExpiration));
            routerProxy.Connection1.Add(ApiKey.Metadata, async _ => await FakeRouter.DefaultMetadataResponse());
            Assert.ThrowsAsync<RequestException>(async () => await router.SendAsync(new FetchRequest(), FakeRouter.TestTopic, PartitionId, CancellationToken.None));
        }

        [Test]
        public async Task ShouldUpdateMetadataOnce()
        {
            var routerProxy = new FakeRouter();
            var cacheExpiration = TimeSpan.FromMilliseconds(10);
            var router = routerProxy.Create(cacheExpiration);

            routerProxy.Connection1.Add(ApiKey.Fetch, ShouldReturnValidMessage);
            routerProxy.Connection1.Add(ApiKey.Metadata, async _ => await FakeRouter.DefaultMetadataResponse());
            int numberOfCall = 1000;
            Task[] tasks = new Task[numberOfCall];
            for (int i = 0; i < numberOfCall / 2; i++)
            {
                tasks[i] = router.SendAsync(new FetchRequest(), FakeRouter.TestTopic, PartitionId, CancellationToken.None);
            }
            await Task.Delay(cacheExpiration);
            await Task.Delay(1);
            for (int i = 0; i < numberOfCall / 2; i++)
            {
                tasks[i + numberOfCall / 2] = router.SendAsync(new FetchRequest(), FakeRouter.TestTopic, PartitionId, CancellationToken.None);
            }

            await Task.WhenAll(tasks);
            Assert.That(routerProxy.Connection1[ApiKey.Fetch], Is.EqualTo(numberOfCall));
            Assert.That(routerProxy.Connection1[ApiKey.Metadata], Is.EqualTo(1));
        }

        [Test]
        public async Task ShouldRecoverUpdateMetadataForNewTopic()
        {
            var routerProxy = new FakeRouter();
            var cacheExpiration = TimeSpan.FromMilliseconds(100);
            var router = routerProxy.Create(cacheExpiration);

            var fetchRequest = new FetchRequest();

            routerProxy.Connection1.Add(ApiKey.Fetch, ShouldReturnValidMessage);
            routerProxy.Connection1.Add(ApiKey.Metadata, async _ => await FakeRouter.DefaultMetadataResponse());
            int numberOfCall = 100;
            Task[] tasks = new Task[numberOfCall];
            for (int i = 0; i < numberOfCall / 2; i++)
            {
                tasks[i] = router.SendAsync(fetchRequest, FakeRouter.TestTopic, PartitionId, CancellationToken.None);
            }

            routerProxy.Connection1.Add(ApiKey.Metadata, async _ => {
                var response = await FakeRouter.DefaultMetadataResponse();
                return new MetadataResponse(response.Brokers, response.Topics.Select(t => new MetadataResponse.Topic("test2", t.ErrorCode, t.Partitions)));
            });

            for (int i = 0; i < numberOfCall / 2; i++)
            {
                tasks[i + numberOfCall / 2] = router.SendAsync(fetchRequest, "test2", PartitionId, CancellationToken.None);
            }

            await Task.WhenAll(tasks);
            Assert.That(routerProxy.Connection1[ApiKey.Fetch], Is.EqualTo(numberOfCall));
            Assert.That(routerProxy.Connection1[ApiKey.Metadata], Is.EqualTo(2));
        }

        [Test]
        public async Task ShouldRecoverFromFailureByUpdateMetadataOnce() //Do not debug this test !!
        {
            var routerProxy = new FakeRouter();
            var cacheExpiration = TimeSpan.FromMilliseconds(1000);
            var router = routerProxy.Create(cacheExpiration);

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
                    throw new ConnectionException("");
                }
                log.Debug(() => LogEvent.Create("Completed "));

                return new FetchResponse();
            };

            routerProxy.Connection1.Add(ApiKey.Fetch, ShouldReturnNotLeaderForPartitionAndThenNoError);
            routerProxy.Connection1.Add(ApiKey.Metadata, async _ => await FakeRouter.DefaultMetadataResponse());

            Task[] tasks = new Task[numberOfCall];

            for (int i = 0; i < numberOfCall; i++)
            {
                tasks[i] = router.SendAsync(fetchRequest, FakeRouter.TestTopic, partitionId, CancellationToken.None);
            }

            await Task.WhenAll(tasks);
            Assert.That(numberOfErrorSend, Is.GreaterThan(1), "numberOfErrorSend");
            Assert.That(routerProxy.Connection1[ApiKey.Fetch], Is.EqualTo(numberOfCall + numberOfErrorSend),
                "RequestCallCount(ApiKey.Fetch)");
            Assert.That(routerProxy.Connection1[ApiKey.Metadata], Is.EqualTo(2), "RequestCallCount(ApiKey.Metadata)");
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
                FailedInFirstMessageError(ErrorCode.LeaderNotAvailable, TimeSpan.Zero));
        }

        /// <summary>
        /// Do not debug this test !!
        /// </summary>
        private async Task ShouldRecoverByUpdateMetadataOnceFullScenario(Func<IRequestContext, Task<IResponse>> fetchResponse) 
        {
            var routerProxy = new FakeRouter();
            var cacheExpiration = TimeSpan.Zero;
            var router = routerProxy.Create(cacheExpiration);
            int partitionId = 0;
            var fetchRequest = new FetchRequest();

            CreateSuccessfulSendMock(routerProxy);

            //Send Successful Message
            await router.SendAsync(fetchRequest, FakeRouter.TestTopic, partitionId, CancellationToken.None);

            Assert.That(routerProxy.Connection1[ApiKey.Fetch], Is.EqualTo(1), "RequestCallCount(ApiKey.Fetch)");
            Assert.That(routerProxy.Connection1[ApiKey.Metadata], Is.EqualTo(1), "RequestCallCount(ApiKey.Metadata)");
            Assert.That(routerProxy.Connection2[ApiKey.Metadata], Is.EqualTo(0), "RequestCallCount(ApiKey.Metadata)");

            routerProxy.Connection1.Add(ApiKey.Fetch, fetchResponse);
            //triger to update metadata
            routerProxy.Connection1.Add(ApiKey.Metadata, async _ => await FakeRouter.MetaResponseWithException());
            routerProxy.Connection2.Add(ApiKey.Metadata, async _ => await FakeRouter.MetadataResponseWithSingleBroker());

            //Reset variables
            routerProxy.Connection1[ApiKey.Fetch] = 0;
            routerProxy.Connection2[ApiKey.Fetch] = 0;
            routerProxy.Connection1[ApiKey.Metadata] = 0;
            routerProxy.Connection2[ApiKey.Metadata] = 0;

            //Send Successful Message that was recover from exception
            await router.SendAsync(fetchRequest, FakeRouter.TestTopic, partitionId, CancellationToken.None);

            Assert.That(routerProxy.Connection1[ApiKey.Fetch], Is.EqualTo(1), "RequestCallCount(ApiKey.Fetch)");
            Assert.That(routerProxy.Connection1[ApiKey.Metadata], Is.EqualTo(1), "RequestCallCount(ApiKey.Metadata)");

            Assert.That(routerProxy.Connection2[ApiKey.Fetch], Is.EqualTo(1), "RequestCallCount(ApiKey.Fetch)");
            Assert.That(routerProxy.Connection2[ApiKey.Metadata], Is.EqualTo(1), "RequestCallCount(ApiKey.Metadata)");
        }


        private static Func<IRequestContext, Task<IResponse>> FailedInFirstMessageError(ErrorCode errorCode, TimeSpan delay)
        {
            return async context => {
                if (context.CorrelationId == 1) {
                    await Task.Delay(delay);
                    await Task.Delay(1);
                    return new FetchResponse(new []{ new FetchResponse.Topic("foo", 1, 0, errorCode)});
                }
                return new FetchResponse();
            };
        }

        private Func<IRequestContext, Task<IResponse>> FailedInFirstMessageException(Type exceptionType, TimeSpan delay)
        {
            return async context =>
            {
                if (context.CorrelationId == 1) {
                    await Task.Delay(delay);
                    await Task.Delay(1);
                    if (exceptionType == typeof(ConnectionException)) {
                        throw new ConnectionException("");
                    }
                    object[] args = new object[1];
                    args[0] = "error Test";
                    throw (Exception)Activator.CreateInstance(exceptionType, args);
                }
                return new FetchResponse();
            };
        }

        private void CreateSuccessfulSendMock(FakeRouter router)
        {
            router.Connection1.Add(ApiKey.Fetch, ShouldReturnValidMessage);
            router.Connection1.Add(ApiKey.Metadata, async _ => await FakeRouter.DefaultMetadataResponse());
            router.Connection2.Add(ApiKey.Fetch, ShouldReturnValidMessage);
            router.Connection2.Add(ApiKey.Metadata, async _ => await FakeRouter.DefaultMetadataResponse());
        }

        private Task<IResponse> ShouldReturnValidMessage(IRequestContext context)
        {
            return Task.FromResult((IResponse)new FetchResponse());
        }
    }
}