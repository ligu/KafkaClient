﻿using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Protocol;
using NSubstitute;
using NUnit.Framework;

#pragma warning disable 1998

namespace KafkaClient.Tests.Unit
{
    [TestFixture]
    public class ProducerTests
    {
        #region SendMessagesAsync Tests...

        [Test]
        public async Task ProducerShouldGroupMessagesByBroker()
        {
            var scenario = new RoutingScenario();
            var router = scenario.CreateRouter();
            using (var producer = new Producer(router))
            {
                var messages = new List<Message>
                {
                    new Message("1"), new Message("2")
                };

                var response = await producer.SendMessagesAsync(messages, "UnitTest", CancellationToken.None);

                Assert.That(scenario.Connection1[ApiKey.Produce], Is.EqualTo(1));
                Assert.That(scenario.Connection2[ApiKey.Produce], Is.EqualTo(1));
            }
        }

        [Test]
        public void ShouldSendAsyncToAllConnectionsEvenWhenExceptionOccursOnOne()
        {
            var scenario = new RoutingScenario();
            scenario.Connection2.Add(ApiKey.Produce, _ => { throw new RequestException(ApiKey.Produce, ErrorCode.CORRUPT_MESSAGE, scenario.Connection2.Endpoint, "some exception"); });
            var router = scenario.CreateRouter();

            using (var producer = new Producer(router))
            {
                var messages = new List<Message> { new Message("1"), new Message("2") };

                var sendTask = producer.SendMessagesAsync(messages, "UnitTest", CancellationToken.None).ConfigureAwait(false);
                Assert.ThrowsAsync<RequestException>(async () => await sendTask);

                Assert.That(scenario.Connection1[ApiKey.Produce], Is.EqualTo(1));
                Assert.That(scenario.Connection2[ApiKey.Produce], Is.EqualTo(1));
            }
        }

        [Test]
        public async Task ProducerShouldReportCorrectNumberOfAsyncRequests()
        {
            var semaphore = new SemaphoreSlim(0);
            var scenario = new RoutingScenario();
            //block the second call returning from send message async
            scenario.Connection1.Add(ApiKey.Produce, async _ =>
            {
                await semaphore.WaitAsync();
                return new ProduceResponse();
            });

            var router = scenario.CreateRouter();
            using (var producer = new Producer(router, new ProducerConfiguration(requestParallelization: 1, batchSize: 1)))
            {
                var messages = new[] { new Message("1") };

                Assert.That(producer.ActiveSenders, Is.EqualTo(0));

                var sendTask = producer.SendMessagesAsync(messages, RoutingScenario.TestTopic, CancellationToken.None);

                await AssertAsync.ThatEventually(() => producer.ActiveSenders >= 1, () => $"senders {producer.ActiveSenders}");

                semaphore.Release();
                await Task.WhenAny(sendTask, Task.Delay(2500));
                Assert.That(sendTask.IsCompleted, Is.True, "Send task should be marked as completed.");
                Assert.That(producer.ActiveSenders, Is.EqualTo(0), "Async should now show zero count.");
            }
        }

        [Test]
        public async Task SendAsyncShouldBlockWhenMaximumAsyncQueueReached()
        {
            TestConfig.Log.Info(() => LogEvent.Create("Start SendAsyncShouldBlockWhenMaximumAsyncQueueReached"));
            int count = 0;
            var semaphore = new SemaphoreSlim(0);
            var scenario = new RoutingScenario();
            //block the second call returning from send message async
            scenario.Connection1.Add(ApiKey.Produce, 
                async _ => {
                    await semaphore.WaitAsync();
                    return new ProduceResponse();
                });

            var router = scenario.CreateRouter();
            using (var producer = new Producer(router, new ProducerConfiguration(requestParallelization: 1, batchSize: 1)))
            {
                var messages = new[] { new Message("1") };

                var task = Task.Run(async () =>
                {
                    var t = producer.SendMessagesAsync(messages, RoutingScenario.TestTopic, CancellationToken.None);
                    Interlocked.Increment(ref count);
                    await t;

                    t = producer.SendMessagesAsync(messages, RoutingScenario.TestTopic, CancellationToken.None);

                    Interlocked.Increment(ref count);
                    await t;
                });

                await AssertAsync.ThatEventually(() => producer.ActiveSenders == 1 && count > 0, () => $"senders {producer.ActiveSenders}, count {count}");

                semaphore.Release();
                // The second SendMessagesAsync should continue after semaphore is released.
                await AssertAsync.ThatEventually(() => count > 1, () => $"count {count}");
            }
        }

        #endregion SendMessagesAsync Tests...

        #region Nagle Tests...

        [Test]
        public async Task ProducesShouldBatchAndOnlySendOneProduceRequest()
        {
            var scenario = new RoutingScenario();
            var producer = new Producer(scenario.CreateRouter(), new ProducerConfiguration(batchSize: 2));
            using (producer)
            {
                var calls = new[]
                {
                    producer.SendMessageAsync(new Message("1"), RoutingScenario.TestTopic, CancellationToken.None),
                    producer.SendMessageAsync(new Message("2"), RoutingScenario.TestTopic, CancellationToken.None)
                };

                await Task.WhenAll(calls);

                Assert.That(scenario.Connection1[ApiKey.Produce], Is.EqualTo(1));
                Assert.That(scenario.Connection2[ApiKey.Produce], Is.EqualTo(1));
            }
        }

        [Test]
        public async Task ProducesShouldSendOneProduceRequestForEachBatchSize()
        {
            var scenario = new RoutingScenario();
            var producer = new Producer(scenario.CreateRouter(), new ProducerConfiguration(batchSize: 4));
            using (producer)
            {
                var calls = new[]
                {
                    producer.SendMessageAsync(new Message("1"), RoutingScenario.TestTopic, CancellationToken.None),
                    producer.SendMessageAsync(new Message("2"), RoutingScenario.TestTopic, CancellationToken.None),
                    producer.SendMessageAsync(new Message("3"), RoutingScenario.TestTopic, CancellationToken.None),
                    producer.SendMessageAsync(new Message("4"), RoutingScenario.TestTopic, CancellationToken.None),
                    producer.SendMessageAsync(new Message("5"), RoutingScenario.TestTopic, CancellationToken.None),
                    producer.SendMessageAsync(new Message("6"), RoutingScenario.TestTopic, CancellationToken.None),
                    producer.SendMessageAsync(new Message("7"), RoutingScenario.TestTopic, CancellationToken.None),
                    producer.SendMessageAsync(new Message("8"), RoutingScenario.TestTopic, CancellationToken.None)
                };

                await Task.WhenAll(calls);

                Assert.That(scenario.Connection1[ApiKey.Produce], Is.EqualTo(2));
                Assert.That(scenario.Connection2[ApiKey.Produce], Is.EqualTo(2));
            }
        }

        [Test]
        [TestCase(1, 2, 100, 100, 2)]
        [TestCase(1, 1, 100, 200, 2)]
        [TestCase(1, 1, 100, 100, 1)]
        public async Task ProducesShouldSendExpectedProduceRequestForEachAckLevelAndTimeoutCombination(short ack1, short ack2, int time1, int time2, int expected)
        {
            var scenario = new RoutingScenario();
            var producer = new Producer(scenario.CreateRouter(), new ProducerConfiguration(batchSize: 100));
            using (producer)
            {
                var calls = new[]
                {
                    producer.SendMessagesAsync(new[] {new Message("1"), new Message("2")}, RoutingScenario.TestTopic, new SendMessageConfiguration(ack1, TimeSpan.FromMilliseconds(time1)), CancellationToken.None),
                    producer.SendMessagesAsync(new[] {new Message("1"), new Message("2")}, RoutingScenario.TestTopic, new SendMessageConfiguration(ack2, TimeSpan.FromMilliseconds(time2)), CancellationToken.None)
                };

                await Task.WhenAll(calls);

                Assert.That(scenario.Connection1[ApiKey.Produce], Is.EqualTo(expected));
                Assert.That(scenario.Connection2[ApiKey.Produce], Is.EqualTo(expected));
            }
        }

        [Test]
        [TestCase(MessageCodec.Gzip, MessageCodec.None, 2)]
        [TestCase(MessageCodec.Gzip, MessageCodec.Gzip, 1)]
        [TestCase(MessageCodec.None, MessageCodec.None, 1)]
        public async Task ProducesShouldSendExpectedProduceRequestForEachCodecCombination(MessageCodec codec1, MessageCodec codec2, int expected)
        {
            var scenario = new RoutingScenario();
            var producer = new Producer(scenario.CreateRouter(), new ProducerConfiguration(batchSize: 100));
            using (producer)
            {
                var calls = new[]
                {
                    producer.SendMessagesAsync(new[] {new Message("1"), new Message("2")}, RoutingScenario.TestTopic, new SendMessageConfiguration(codec: codec1), CancellationToken.None),
                    producer.SendMessagesAsync(new[] {new Message("1"), new Message("2")}, RoutingScenario.TestTopic, new SendMessageConfiguration(codec: codec2), CancellationToken.None)
                };

                await Task.WhenAll(calls);

                Assert.That(scenario.Connection1[ApiKey.Produce], Is.EqualTo(expected));
                Assert.That(scenario.Connection2[ApiKey.Produce], Is.EqualTo(expected));
            }
        }

        [Test]
        public async Task ProducerShouldAllowFullBatchSizeOfMessagesToQueue()
        {
            var scenario = new RoutingScenario();
            var producer = new Producer(scenario.CreateRouter(), new ProducerConfiguration(batchSize: 1002, batchMaxDelay: TimeSpan.FromSeconds(10000)));

            using (producer)
            {
                var count = 1000;

                var senderTask = Task.Run(() => {
                    for (var i = 0; i < count; i++) {
                        producer.SendMessageAsync(new Message(i.ToString()), RoutingScenario.TestTopic, CancellationToken.None);
                    }
                });
                await senderTask;
                TestConfig.Log.Info(() => LogEvent.Create("Finished test send task"));

                Assert.That(senderTask.IsCompleted);
                await AssertAsync.ThatEventually(() => producer.InFlightMessageCount + producer.BufferedMessageCount == count, () => $"in flight {producer.InFlightMessageCount}, buffered {producer.BufferedMessageCount}, total {count}");
            }
        }

        [Test]
        //someTime failed
        public async Task ProducerShouldBlockWhenFullBufferReached()
        {
            int count = 0;
            //with max buffer set below the batch size, this should cause the producer to block until batch delay time.
            var scenario = new RoutingScenario();
            scenario.Connection1.Add(ApiKey.Produce, async _ => {
                await Task.Delay(200);
                return new ProduceResponse();
            });
            using (var producer = new Producer(scenario.CreateRouter(), new ProducerConfiguration(batchSize: 10, batchMaxDelay: TimeSpan.FromMilliseconds(500))))
            {
                var senderTask = Task.Factory.StartNew(async () => {
                    for (int i = 0; i < 3; i++) {
                        await producer.SendMessageAsync(new Message(i.ToString()), RoutingScenario.TestTopic, CancellationToken.None);
                        TestConfig.Log.Info(() => LogEvent.Create($"Buffered {producer.BufferedMessageCount}, In Flight: {producer.InFlightMessageCount}"));
                        Interlocked.Increment(ref count);
                    }
                });

                await AssertAsync.ThatEventually(() => count > 0 && producer.BufferedMessageCount == 1, () => $"buffered {producer.BufferedMessageCount}, count {count}");

                TestConfig.Log.Info(() => LogEvent.Create("Waiting for the rest..."));
                await Task.WhenAny(senderTask, Task.Delay(5000));

                Assert.That(senderTask.IsCompleted);
                Assert.That(producer.BufferedMessageCount, Is.EqualTo(1), "One message should be left in the buffer.");
            }
        }

        #endregion Nagle Tests...

        #region Dispose Tests...

        [Test]
        public async Task SendingMessageWhenDisposedShouldThrow()
        {
            var router = Substitute.For<IRouter>();
            var producer = new Producer(router);
            using (producer) { }
            Assert.ThrowsAsync<ObjectDisposedException>(async () => await producer.SendMessageAsync(new Message("1"), "Test", CancellationToken.None));
        }

        [Test]
        public async Task EnsureProducerDisposesRouter()
        {
            var router = Substitute.For<IRouter>();

            var producer = new Producer(router, new ProducerConfiguration(stopTimeout: TimeSpan.FromMilliseconds(5)), leaveRouterOpen: false);
            await producer.UsingAsync(() => { });
            router.Received(1).Dispose();
        }

        [Test]
        public async Task EnsureProducerDoesNotDisposeRouter()
        {
            var router = Substitute.For<IRouter>();

            var producer = new Producer(router, new ProducerConfiguration(stopTimeout: TimeSpan.FromMilliseconds(5)));
            await producer.UsingAsync(() => { });
            router.DidNotReceive().Dispose();
        }

        [Test]
        public void ProducerShouldInterruptWaitOnEmptyCollection()
        {
            //use the fake to actually cause loop to execute
            var router = new RoutingScenario().CreateRouter();

            var producer = new Producer(router);
            using (producer) { }
        }

        #endregion Dispose Tests...
    }
}