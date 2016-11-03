using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Protocol;
using KafkaClient.Tests.Fakes;
using KafkaClient.Tests.Helpers;
using NSubstitute;
using NUnit.Framework;

namespace KafkaClient.Tests
{
    [TestFixture]
    [Category("Unit")]
    public class ConsumerTests
    {
        [Test]
        public void CancellationShouldInterruptConsumption()
        {
            var routerProxy = new BrokerRouterProxy();
#pragma warning disable 1998
            routerProxy.Connection1.FetchResponseFunction = async () => new FetchResponse(new FetchResponse.Topic[] {});
#pragma warning restore 1998

            var router = routerProxy.Create();

            var options = CreateOptions(router);

            using (var consumer = new OldConsumer(options))
            {
                var tokenSrc = new CancellationTokenSource();

                var consumeTask = Task.Run(() => consumer.Consume(tokenSrc.Token).FirstOrDefault());

                //wait until the fake broker is running and requesting fetches
                var wait = TaskTest.WaitFor(() => routerProxy.Connection1.FetchRequestCallCount > 10);

                tokenSrc.Cancel();

                Assert.That(
                    Assert.Throws<AggregateException>(consumeTask.Wait).InnerException,
                    Is.TypeOf<OperationCanceledException>());
            }
        }

        [Test]
        public async Task ConsumerWhitelistShouldOnlyConsumeSpecifiedPartition()
        {
            var routerProxy = new BrokerRouterProxy();
#pragma warning disable 1998
            routerProxy.Connection1.FetchResponseFunction = async () => new FetchResponse(new FetchResponse.Topic[] {});
#pragma warning restore 1998
            var router = routerProxy.Create();
            var options = CreateOptions(router);
            options.PartitionWhitelist = new List<int> { 0 };
            using (var consumer = new OldConsumer(options))
            {
                var test = consumer.Consume();

                await TaskTest.WaitFor(() => consumer.ConsumerTaskCount > 0);
                await TaskTest.WaitFor(() => routerProxy.Connection1.FetchRequestCallCount > 0);

                Assert.That(consumer.ConsumerTaskCount, Is.EqualTo(1),
                    "Consumer should only create one consuming thread for partition 0.");
                Assert.That(routerProxy.Connection1.FetchRequestCallCount, Is.GreaterThanOrEqualTo(1));
                Assert.That(routerProxy.Connection2.FetchRequestCallCount, Is.EqualTo(0));
            }
        }

        [Test]
        public async Task ConsumerWithEmptyWhitelistShouldConsumeAllPartition()
        {
            var routerProxy = new BrokerRouterProxy();

            var router = routerProxy.Create();
            var options = CreateOptions(router);
            options.PartitionWhitelist = new List<int>();

            using (var consumer = new OldConsumer(options))
            {
                var test = consumer.Consume();

                await TaskTest.WaitFor(() => consumer.ConsumerTaskCount > 0);
                await TaskTest.WaitFor(() => routerProxy.Connection1.FetchRequestCallCount > 0);
                await TaskTest.WaitFor(() => routerProxy.Connection2.FetchRequestCallCount > 0);

                Assert.That(consumer.ConsumerTaskCount, Is.EqualTo(2),
                    "Consumer should create one consuming thread for each partition.");
                Assert.That(routerProxy.Connection1.FetchRequestCallCount, Is.GreaterThanOrEqualTo(1),
                    "Connection1 not sent FetchRequest");
                Assert.That(routerProxy.Connection2.FetchRequestCallCount, Is.GreaterThanOrEqualTo(1),
                    "Connection2 not sent FetchRequest");
            }
        }

        [Test]
        public void ConsumerShouldCreateTaskForEachBroker()
        {
            var routerProxy = new BrokerRouterProxy();
#pragma warning disable 1998
            routerProxy.Connection1.FetchResponseFunction = async () => new FetchResponse(new FetchResponse.Topic[] {});
#pragma warning restore 1998
            var router = routerProxy.Create();
            var options = CreateOptions(router);
            options.PartitionWhitelist = new List<int>();
            using (var consumer = new OldConsumer(options))
            {
                var test = consumer.Consume();
                var wait = TaskTest.WaitFor(() => consumer.ConsumerTaskCount >= 2);

                Assert.That(consumer.ConsumerTaskCount, Is.EqualTo(2));
            }
        }

        [Test]
        public void ConsumerShouldReturnOffset()
        {
            var routerProxy = new BrokerRouterProxy();
#pragma warning disable 1998
            routerProxy.Connection1.FetchResponseFunction = async () => new FetchResponse(new FetchResponse.Topic[] {});
#pragma warning restore 1998
            var router = routerProxy.Create();
            var options = CreateOptions(router);
            options.PartitionWhitelist = new List<int>();
            using (var consumer = new OldConsumer(options))
            {
                var test = consumer.Consume();
                var wait = TaskTest.WaitFor(() => consumer.ConsumerTaskCount >= 2);

                Assert.That(consumer.ConsumerTaskCount, Is.EqualTo(2));
            }
        }

        [Test]
        public void EnsureConsumerDisposesRouter()
        {
            var router = Substitute.For<IBrokerRouter>();
            //router.Setup(x => x.Log.DebugFormat(It.IsAny<string>()));
            var consumer = new OldConsumer(CreateOptions(router));
            using (consumer) { }
            router.Received().Dispose();
        }

        [Test]
        public void EnsureConsumerDisposesAllTasks()
        {
            var routerProxy = new BrokerRouterProxy();
#pragma warning disable 1998
            routerProxy.Connection1.FetchResponseFunction = async () => new FetchResponse(new FetchResponse.Topic[] {});
#pragma warning restore 1998
            var router = routerProxy.Create();
            var options = CreateOptions(router);
            options.PartitionWhitelist = new List<int>();

            var consumer = new OldConsumer(options);
            using (consumer)
            {
                var test = consumer.Consume();
                var wait = TaskTest.WaitFor(() => consumer.ConsumerTaskCount >= 2);
            }

            var wait2 = TaskTest.WaitFor(() => consumer.ConsumerTaskCount <= 0);
            Assert.That(consumer.ConsumerTaskCount, Is.EqualTo(0));
        }

        private ConsumerOptions CreateOptions(IBrokerRouter router)
        {
            return new ConsumerOptions(BrokerRouterProxy.TestTopic, router);
        }
    }
}