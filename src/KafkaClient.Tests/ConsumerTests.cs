using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Connections;
using KafkaClient.Protocol;
using KafkaClient.Protocol.Types;
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
        //        [Test]
        //        public async Task CancellationShouldInterruptConsumption()
        //        {
        //            var routerProxy = new BrokerRouterProxy();
        //#pragma warning disable 1998
        //            routerProxy.Connection1.FetchResponseFunction = async () => new FetchResponse(new FetchResponse.Topic[] {});
        //#pragma warning restore 1998

        //            var router = routerProxy.Create();
        //            var consumer = new Consumer(router);
        //            var tokenSrc = new CancellationTokenSource();

        //            var consumeTask = consumer.FetchMessagesAsync("TestTopic", 0, 0, 2048, tokenSrc.Token);

        //                //wait until the fake broker is running and requesting fetches
        //            var wait = await TaskTest.WaitFor(() => routerProxy.Connection1.FetchRequestCallCount > 10);

        //            tokenSrc.Cancel();

        //            try {
        //                await consumeTask;
        //                Assert.Fail("Should throw OperationFailedException");
        //            } catch (AggregateException ex) when (ex.InnerException is OperationCanceledException) {
        //            }
        //        }

        //        [Test]
        //        public async Task ConsumerWhitelistShouldOnlyConsumeSpecifiedPartition()
        //        {
        //            var routerProxy = new BrokerRouterProxy();
        //#pragma warning disable 1998
        //            routerProxy.Connection1.FetchResponseFunction = async () => new FetchResponse(new FetchResponse.Topic[] {});
        //#pragma warning restore 1998
        //            var router = routerProxy.Create();
        //            var options = CreateOptions(router);
        //            options.PartitionWhitelist = new List<int> { 0 };
        //            using (var consumer = new OldConsumer(options))
        //            {
        //                var test = consumer.Consume();

        //                await TaskTest.WaitFor(() => consumer.ConsumerTaskCount > 0);
        //                await TaskTest.WaitFor(() => routerProxy.Connection1.FetchRequestCallCount > 0);

        //                Assert.That(consumer.ConsumerTaskCount, Is.EqualTo(1),
        //                    "Consumer should only create one consuming thread for partition 0.");
        //                Assert.That(routerProxy.Connection1.FetchRequestCallCount, Is.GreaterThanOrEqualTo(1));
        //                Assert.That(routerProxy.Connection2.FetchRequestCallCount, Is.EqualTo(0));
        //            }
        //        }

        //        [Test]
        //        public async Task ConsumerWithEmptyWhitelistShouldConsumeAllPartition()
        //        {
        //            var routerProxy = new BrokerRouterProxy();

        //            var router = routerProxy.Create();
        //            var options = CreateOptions(router);
        //            options.PartitionWhitelist = new List<int>();

        //            using (var consumer = new OldConsumer(options))
        //            {
        //                var test = consumer.Consume();

        //                await TaskTest.WaitFor(() => consumer.ConsumerTaskCount > 0);
        //                await TaskTest.WaitFor(() => routerProxy.Connection1.FetchRequestCallCount > 0);
        //                await TaskTest.WaitFor(() => routerProxy.Connection2.FetchRequestCallCount > 0);

        //                Assert.That(consumer.ConsumerTaskCount, Is.EqualTo(2),
        //                    "Consumer should create one consuming thread for each partition.");
        //                Assert.That(routerProxy.Connection1.FetchRequestCallCount, Is.GreaterThanOrEqualTo(1),
        //                    "Connection1 not sent FetchRequest");
        //                Assert.That(routerProxy.Connection2.FetchRequestCallCount, Is.GreaterThanOrEqualTo(1),
        //                    "Connection2 not sent FetchRequest");
        //            }
        //        }

        //        [Test]
        //        public void ConsumerShouldCreateTaskForEachBroker()
        //        {
        //            var routerProxy = new BrokerRouterProxy();
        //#pragma warning disable 1998
        //            routerProxy.Connection1.FetchResponseFunction = async () => new FetchResponse(new FetchResponse.Topic[] {});
        //#pragma warning restore 1998
        //            var router = routerProxy.Create();
        //            var options = CreateOptions(router);
        //            options.PartitionWhitelist = new List<int>();
        //            using (var consumer = new OldConsumer(options))
        //            {
        //                var test = consumer.Consume();
        //                var wait = TaskTest.WaitFor(() => consumer.ConsumerTaskCount >= 2);

        //                Assert.That(consumer.ConsumerTaskCount, Is.EqualTo(2));
        //            }
        //        }

        //        [Test]
        //        public void ConsumerShouldReturnOffset()
        //        {
        //            var routerProxy = new BrokerRouterProxy();
        //#pragma warning disable 1998
        //            routerProxy.Connection1.FetchResponseFunction = async () => new FetchResponse(new FetchResponse.Topic[] {});
        //#pragma warning restore 1998
        //            var router = routerProxy.Create();
        //            var options = CreateOptions(router);
        //            options.PartitionWhitelist = new List<int>();
        //            using (var consumer = new OldConsumer(options))
        //            {
        //                var test = consumer.Consume();
        //                var wait = TaskTest.WaitFor(() => consumer.ConsumerTaskCount >= 2);

        //                Assert.That(consumer.ConsumerTaskCount, Is.EqualTo(2));
        //            }
        //        }

        [Test]
        public void EnsureConsumerDisposesRouter()
        {
            var router = Substitute.For<IRouter>();

            var consumer = new Consumer(router, leaveRouterOpen: false);
            using (consumer) { }
            router.Received(1).Dispose();
        }

        [Test]
        public void EnsureConsumerDoesNotDisposeRouter()
        {
            var router = Substitute.For<IRouter>();

            var consumer = new Consumer(router);
            using (consumer) { }
            router.DidNotReceive().Dispose();
        }

        [Test]
        public async Task ConsumerThowsArgumentExceptionWhenMemberMetadataIsNotKnownByConsumer([Values(null, "", "unknown")] string protocolType)
        {
            var router = Substitute.For<IRouter>();

            var consumer = new Consumer(router);
            using (consumer) {
                try {
                    await consumer.JoinConsumerGroupAsync("group", new ByteMemberMetadata(protocolType, new byte[] { }), CancellationToken.None);
                    Assert.Fail("Should have thrown exception");
                } catch (ArgumentOutOfRangeException ex) {
                    Assert.That(ex.Message, Is.EqualTo("ProtocolType be known by Encoders\r\nParameter name: metadata"));
                }
            }
        }

        [Test]
        public async Task ConsumerDoesNotThowArgumentExceptionWhenMemberMetadataIsKnownByConsumer()
        {
            var router = Substitute.For<IRouter>();
            var conn = Substitute.For<IConnection>();
            router.GetGroupBrokerAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                  .Returns(_ => Task.FromResult(new GroupBroker(_.Arg<string>(), 0, conn)));

            var consumer = new Consumer(router, encoders: ImmutableDictionary<string, IProtocolTypeEncoder>.Empty.Add(ConsumerEncoder.ProtocolType, ConsumerEncoder.Singleton));
            using (consumer) {
                try {
                    await consumer.JoinConsumerGroupAsync("group", new ByteMemberMetadata(ConsumerEncoder.ProtocolType, new byte[] { }), CancellationToken.None);
                } catch (RequestException) {
                    // since the servers aren't available
                }
            }
        }

        //        [Test]
        //        public void EnsureConsumerDisposesAllTasks()
        //        {
        //            var routerProxy = new BrokerRouterProxy();
        //#pragma warning disable 1998
        //            routerProxy.Connection1.FetchResponseFunction = async () => new FetchResponse(new FetchResponse.Topic[] {});
        //#pragma warning restore 1998
        //            var router = routerProxy.Create();
        //            var options = CreateOptions(router);
        //            options.PartitionWhitelist = new List<int>();

        //            var consumer = new OldConsumer(options);
        //            using (consumer)
        //            {
        //                var test = consumer.Consume();
        //                var wait = TaskTest.WaitFor(() => consumer.ConsumerTaskCount >= 2);
        //            }

        //            var wait2 = TaskTest.WaitFor(() => consumer.ConsumerTaskCount <= 0);
        //            Assert.That(consumer.ConsumerTaskCount, Is.EqualTo(0));
        //        }

        //        private ConsumerOptions CreateOptions(IRouter router)
        //        {
        //            return new ConsumerOptions(BrokerRouterProxy.TestTopic, router);
        //        }
    }
}