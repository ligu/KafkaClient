using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Protocol;
using KafkaClient.Tests.Fakes;
using KafkaClient.Tests.Helpers;
using NSubstitute;
using NUnit.Framework;

namespace KafkaClient.Tests.Unit
{
    [TestFixture]
    [Category("Unit")]
    public class ProducerTests
    {
        #region SendMessagesAsync Tests...

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void ProducerShouldGroupMessagesByBroker()
        {
            var routerProxy = new FakeBrokerRouter();
            var router = routerProxy.Create();
            using (var producer = new Producer(router))
            {
                var messages = new List<Message>
                {
                    new Message("1"), new Message("2")
                };

                var response = producer.SendMessagesAsync(messages, "UnitTest", CancellationToken.None).Result;

                Assert.That(response.Length, Is.EqualTo(2));
                Assert.That(routerProxy.BrokerConn0.ProduceRequestCallCount, Is.EqualTo(1));
                Assert.That(routerProxy.BrokerConn1.ProduceRequestCallCount, Is.EqualTo(1));
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void ShouldSendAsyncToAllConnectionsEvenWhenExceptionOccursOnOne()
        {
            var routerProxy = new FakeBrokerRouter();
            routerProxy.BrokerConn1.ProduceResponseFunction = () => { throw new RequestException("some exception"); };
            var router = routerProxy.Create();

            using (var producer = new Producer(router))
            {
                var messages = new List<Message> { new Message("1"), new Message("2") };

                var sendTask = producer.SendMessagesAsync(messages, "UnitTest", CancellationToken.None).ConfigureAwait(false);
                Assert.Throws<RequestException>(async () => await sendTask);

                Assert.That(routerProxy.BrokerConn0.ProduceRequestCallCount, Is.EqualTo(1));
                Assert.That(routerProxy.BrokerConn1.ProduceRequestCallCount, Is.EqualTo(1));
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task ProducerShouldReportCorrectAmountOfAsyncRequests()
        {
            // var log = new ConsoleLog(); for (int i = 0; i < 100; i++) {
            var semaphore = new SemaphoreSlim(0);
            var routerProxy = new FakeBrokerRouter();
            //block the second call returning from send message async
            routerProxy.BrokerConn0.ProduceResponseFunction = async () =>
            {
                await semaphore.WaitAsync();
                return new ProduceResponse();
            };

            var router = routerProxy.Create();
            using (var producer = new Producer(router, new ProducerConfiguration(requestParallelization: 1, batchSize: 1)))
            {
                var messages = new[] { new Message("1") };

                Assert.That(producer.ActiveSenders, Is.EqualTo(0));

                var sendTask = producer.SendMessagesAsync(messages, BrokerRouterProxy.TestTopic, CancellationToken.None);

                await TaskTest.WaitFor(() => producer.ActiveSenders > 0);
                Assert.That(producer.ActiveSenders, Is.EqualTo(1), "One async operation should be sending.");

                semaphore.Release();
                sendTask.Wait(TimeSpan.FromMilliseconds(500));
                await Task.Delay(2);
                Assert.That(sendTask.IsCompleted, Is.True, "Send task should be marked as completed.");
                Assert.That(producer.ActiveSenders, Is.EqualTo(0), "Async should now show zero count.");
                // } log.DebugFormat(i.ToString());
            }
        }

        private readonly ILog _log = new TraceLog();

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task SendAsyncShouldBlockWhenMaximumAsyncQueueReached()
        {
            _log.Info(() => LogEvent.Create("Start SendAsyncShouldBlockWhenMaximumAsyncQueueReached"));
            int count = 0;
            var semaphore = new SemaphoreSlim(0);
            var routerProxy = new FakeBrokerRouter();
            //block the second call returning from send message async
            routerProxy.BrokerConn0.ProduceResponseFunction = async () => { await semaphore.WaitAsync(); return new ProduceResponse(); };

            var router = routerProxy.Create();
            using (var producer = new Producer(router, new ProducerConfiguration(requestParallelization: 1, batchSize: 1)))
            {
                var messages = new[] { new Message("1") };

                var task = Task.Run(async () =>
                {
                    var t = producer.SendMessagesAsync(messages, BrokerRouterProxy.TestTopic, CancellationToken.None);
                    Interlocked.Increment(ref count);
                    await t;

                    t = producer.SendMessagesAsync(messages, BrokerRouterProxy.TestTopic, CancellationToken.None);

                    Interlocked.Increment(ref count);
                    await t;
                });

                await TaskTest.WaitFor(() => producer.ActiveSenders > 0);
                await TaskTest.WaitFor(() => count > 0);

                Assert.That(count, Is.EqualTo(1), "Only one SendMessagesAsync should continue.");

                semaphore.Release();
                await TaskTest.WaitFor(() => count > 1);
                Assert.That(count, Is.EqualTo(2), "The second SendMessagesAsync should continue after semaphore is released.");
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [Ignore("is there a way to communicate back which client failed and which succeeded.")]
        public void ConnectionExceptionOnOneShouldCommunicateBackWhichMessagesFailed()
        {
            //TODO is there a way to communicate back which client failed and which succeeded.
            var routerProxy = new FakeBrokerRouter();
            routerProxy.BrokerConn1.ProduceResponseFunction = () => { throw new ApplicationException("some exception"); };

            var router = routerProxy.Create();
            using (var producer = new Producer(router))
            {
                var messages = new List<Message>
                {
                    new Message("1"), new Message("2")
                };

                //this will produce an exception, but message 1 succeeded and message 2 did not.
                //should we return a ProduceResponse with an error and no error for the other messages?
                //at this point though the client does not know which message is routed to which server.
                //the whole batch of messages would need to be returned.
                var test = producer.SendMessagesAsync(messages, "UnitTest", CancellationToken.None).Result;
            }
        }

        #endregion SendMessagesAsync Tests...

        #region Nagle Tests...

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async void ProducesShouldBatchAndOnlySendOneProduceRequest()
        {
            var routerProxy = new FakeBrokerRouter();
            var producer = new Producer(routerProxy.Create(), new ProducerConfiguration(batchSize: 2));
            using (producer)
            {
                var calls = new[]
                {
                    producer.SendMessageAsync(new Message("1"), FakeBrokerRouter.TestTopic, CancellationToken.None),
                    producer.SendMessageAsync(new Message("2"), FakeBrokerRouter.TestTopic, CancellationToken.None)
                };

                await Task.WhenAll(calls);

                Assert.That(routerProxy.BrokerConn0.ProduceRequestCallCount, Is.EqualTo(1));
                Assert.That(routerProxy.BrokerConn1.ProduceRequestCallCount, Is.EqualTo(1));
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async void ProducesShouldSendOneProduceRequestForEachBatchSize()
        {
            var routerProxy = new FakeBrokerRouter();
            var producer = new Producer(routerProxy.Create(), new ProducerConfiguration(batchSize: 2));
            using (producer)
            {
                var calls = new[]
                {
                    producer.SendMessageAsync(new Message("1"), FakeBrokerRouter.TestTopic, CancellationToken.None),
                    producer.SendMessageAsync(new Message("2"), FakeBrokerRouter.TestTopic, CancellationToken.None),
                    producer.SendMessageAsync(new Message("3"), FakeBrokerRouter.TestTopic, CancellationToken.None),
                    producer.SendMessageAsync(new Message("4"), FakeBrokerRouter.TestTopic, CancellationToken.None)
                };

                await Task.WhenAll(calls);

                Assert.That(routerProxy.BrokerConn0.ProduceRequestCallCount, Is.EqualTo(2));
                Assert.That(routerProxy.BrokerConn1.ProduceRequestCallCount, Is.EqualTo(2));
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [TestCase(1, 2, 100, 100, 2)]
        [TestCase(1, 1, 100, 200, 2)]
        [TestCase(1, 1, 100, 100, 1)]
        public async void ProducesShouldSendExpectedProduceRequestForEachAckLevelAndTimeoutCombination(short ack1, short ack2, int time1, int time2, int expected)
        {
            var routerProxy = new FakeBrokerRouter();
            var producer = new Producer(routerProxy.Create(), new ProducerConfiguration(batchSize: 100));
            using (producer)
            {
                var calls = new[]
                {
                    producer.SendMessagesAsync(new[] {new Message("1"), new Message("2")}, FakeBrokerRouter.TestTopic, null, new SendMessageConfiguration(ack1, TimeSpan.FromMilliseconds(time1)), CancellationToken.None),
                    producer.SendMessagesAsync(new[] {new Message("1"), new Message("2")}, FakeBrokerRouter.TestTopic, null, new SendMessageConfiguration(ack2, TimeSpan.FromMilliseconds(time2)), CancellationToken.None)
                };

                await Task.WhenAll(calls);

                Assert.That(routerProxy.BrokerConn0.ProduceRequestCallCount, Is.EqualTo(expected));
                Assert.That(routerProxy.BrokerConn1.ProduceRequestCallCount, Is.EqualTo(expected));
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [TestCase(MessageCodec.CodecGzip, MessageCodec.CodecNone, 2)]
        [TestCase(MessageCodec.CodecGzip, MessageCodec.CodecGzip, 1)]
        [TestCase(MessageCodec.CodecNone, MessageCodec.CodecNone, 1)]
        public async void ProducesShouldSendExpectedProduceRequestForEachCodecCombination(MessageCodec codec1, MessageCodec codec2, int expected)
        {
            var routerProxy = new FakeBrokerRouter();
            var producer = new Producer(routerProxy.Create(), new ProducerConfiguration(batchSize: 100));
            using (producer)
            {
                var calls = new[]
                {
                    producer.SendMessagesAsync(new[] {new Message("1"), new Message("2")}, FakeBrokerRouter.TestTopic, null, new SendMessageConfiguration(codec: codec1), CancellationToken.None),
                    producer.SendMessagesAsync(new[] {new Message("1"), new Message("2")}, FakeBrokerRouter.TestTopic, null, new SendMessageConfiguration(codec: codec2), CancellationToken.None)
                };

                await Task.WhenAll(calls);

                Assert.That(routerProxy.BrokerConn0.ProduceRequestCallCount, Is.EqualTo(expected));
                Assert.That(routerProxy.BrokerConn1.ProduceRequestCallCount, Is.EqualTo(expected));
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async void ProducerShouldAllowFullBatchSizeOfMessagesToQueue()
        {
            var routerProxy = new FakeBrokerRouter();
            var producer = new Producer(routerProxy.Create(), new ProducerConfiguration(batchSize: 1002, batchMaxDelay: TimeSpan.FromSeconds(10000)));

            using (producer)
            {
                int numberOfTime = 1000;

                var senderTask = Task.Run(() =>
                {
                    for (int i = 0; i < numberOfTime; i++)
                    {
                        producer.SendMessageAsync(new Message(i.ToString()), FakeBrokerRouter.TestTopic, CancellationToken.None);
                    }
                });
                await senderTask;

                Assert.That(senderTask.IsCompleted);
                Assert.That(producer.BufferedMessageCount, Is.EqualTo(1000));
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        //someTime failed
        public async Task ProducerShouldBlockWhenFullBufferReached()
        {
            int count = 0;
            //with max buffer set below the batch size, this should cause the producer to block until batch delay time.
            var routerProxy = new FakeBrokerRouter();
            routerProxy.BrokerConn0.ProduceResponseFunction = async () => {
                await Task.Delay(200);
                return new ProduceResponse();
            };
            using (var producer = new Producer(routerProxy.Create(), new ProducerConfiguration(batchSize: 10, batchMaxDelay: TimeSpan.FromMilliseconds(500))))
            {
                var senderTask = Task.Factory.StartNew(async () => {
                    for (int i = 0; i < 3; i++) {
                        await producer.SendMessageAsync(new Message(i.ToString()), FakeBrokerRouter.TestTopic, CancellationToken.None);
                        Console.WriteLine("Await: {0}", producer.BufferedMessageCount);
                        Interlocked.Increment(ref count);
                    }
                });

                await TaskTest.WaitFor(() => count > 0);
                Assert.That(producer.BufferedMessageCount, Is.EqualTo(1));

                Console.WriteLine("Waiting for the rest...");
                senderTask.Wait(TimeSpan.FromSeconds(5));

                Assert.That(senderTask.IsCompleted);
                Assert.That(producer.BufferedMessageCount, Is.EqualTo(1), "One message should be left in the buffer.");

                Console.WriteLine("Unwinding...");
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [Ignore("Removed the max message limit.  Caused performance problems.  Will find a better way.")]
        public void ProducerShouldBlockEvenOnMessagesInTransit()
        {
            //with max buffer set below the batch size, this should cause the producer to block until batch delay time.
            var routerProxy = new FakeBrokerRouter();
            var semaphore = new SemaphoreSlim(0);
#pragma warning disable 1998
            routerProxy.BrokerConn0.ProduceResponseFunction = async () => { semaphore.Wait(); return new ProduceResponse(); };
            routerProxy.BrokerConn1.ProduceResponseFunction = async () => { semaphore.Wait(); return new ProduceResponse(); };
#pragma warning restore 1998

            var producer = new Producer(routerProxy.Create(), new ProducerConfiguration(1, 1, TimeSpan.FromMilliseconds(500)));
            using (producer)
            {
                var sendTasks = Enumerable.Range(0, 5)
                    .Select(x => producer.SendMessageAsync(new Message(x.ToString()), FakeBrokerRouter.TestTopic, CancellationToken.None))
                    .ToList();

                var wait = TaskTest.WaitFor(() => producer.ActiveSenders > 0);
                Assert.That(sendTasks.Any(x => x.IsCompleted) == false, "All the async tasks should be blocking or in transit.");
                Assert.That(producer.BufferedMessageCount, Is.EqualTo(5), "We sent 5 unfinished messages, they should be counted towards the buffer.");

                semaphore.Release(2);
            }
        }

        #endregion Nagle Tests...

        #region Dispose Tests...

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [ExpectedException(typeof(ObjectDisposedException))]
        public async void SendingMessageWhenDisposedShouldThrow()
        {
            var router = Substitute.For<IBrokerRouter>();
            var producer = new Producer(router);
            using (producer) { }
            await producer.SendMessageAsync(new Message("1"), "Test", CancellationToken.None);
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [ExpectedException(typeof(ObjectDisposedException))]
        public async Task SendingMessageWhenStoppedShouldThrow()
        {
            var router = Substitute.For<IBrokerRouter>();
            using (var producer = new Producer(router))
            {
                await producer.StopAsync(CancellationToken.None);
                await producer.SendMessageAsync(new Message("1"), "Test", CancellationToken.None);
            }
        }

        //[Test,Repeat(IntegrationConfig.NumberOfRepeat)]
        //public async void StopShouldWaitUntilCollectionEmpty()
        //{
        //    var fakeRouter = new FakeBrokerRouter();

        // using (var producer = new Producer(fakeRouter.Create()) { BatchDelayTime =
        // TimeSpan.FromMilliseconds(500) }) { var sendTask =
        // producer.SendMessagesAsync(FakeBrokerRouter.TestTopic, new[] { new Message() });
        // Assert.That(producer.BufferedMessageCount, Is.EqualTo(1));

        // producer.Stop(true, TimeSpan.FromSeconds(5));

        // sendTask; Assert.That(producer.BufferedMessageCount, Is.EqualTo(0));
        // Assert.That(sendTask.IsCompleted, Is.True);

        //        Console.WriteLine("Unwinding test...");
        //    }
        //}

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void EnsureProducerDisposesRouter()
        {
            var router = Substitute.For<IBrokerRouter>();

            var producer = new Producer(router);
            using (producer) { }
            router.Received(1).Dispose();
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void ProducerShouldInterruptWaitOnEmptyCollection()
        {
            //use the fake to actually cause loop to execute
            var router = new FakeBrokerRouter().Create();

            var producer = new Producer(router);
            using (producer) { }
        }

        #endregion Dispose Tests...
    }
}