using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Connection;
using KafkaClient.Tests.Fakes;
using KafkaClient.Tests.Helpers;
using NUnit.Framework;

namespace KafkaClient.Tests.Unit
{
    /// <summary>
    /// Note these integration tests require an actively running kafka server defined in the app.config file.
    /// </summary>
    [TestFixture]
    [Category("Integration")]
    public class KafkaTcpSocketTests
    {
        private const int FakeServerPort = 8999;
        private readonly KafkaEndpoint _fakeServerUrl;
        private readonly KafkaEndpoint _badServerUrl;
        private IKafkaLog _log = new TraceLog(LogLevel.Info);
        private int _maxRetry = 5;

        public KafkaTcpSocketTests()
        {
            _fakeServerUrl = new KafkaConnectionFactory().Resolve(new Uri("http://localhost:8999"), _log);
            _badServerUrl = new KafkaConnectionFactory().Resolve(new Uri("http://localhost:1"), _log);
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void KafkaTcpSocketShouldConstruct()
        {
            using (var test = new KafkaTcpSocket(_log, _fakeServerUrl, _maxRetry))
            {
                Assert.That(test, Is.Not.Null);
                Assert.That(test.Endpoint, Is.EqualTo(_fakeServerUrl));
            }
        }

        #region Connection Tests...

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task ConnectionShouldStartDedicatedThreadOnConstruction()
        {
            var count = 0;

            using (var test = new KafkaTcpSocket(_log, _fakeServerUrl, _maxRetry))
            {
                test.OnReconnectionAttempt += x => Interlocked.Increment(ref count);
                await TaskTest.WaitFor(() => count > 0);
                Assert.That(count, Is.GreaterThan(0));
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task ConnectionShouldAttemptMultipleTimesWhenConnectionFails()
        {
            var count = 0;
            using (var test = new KafkaTcpSocket(_log, _badServerUrl, _maxRetry))
            {
                var write = test.WriteAsync(1.ToBytes().ToPayload()); //will force a connection
                test.OnReconnectionAttempt += x => Interlocked.Increment(ref count);
                await TaskTest.WaitFor(() => count > 1, 10000);
                Assert.That(count, Is.GreaterThan(1));
            }
        }

        #endregion Connection Tests...

        #region Dispose Tests...

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task KafkaTcpSocketShouldDisposeEvenWhilePollingToReconnect()
        {
            int connectionAttempt = 0;
            using (var test = new KafkaTcpSocket(_log, _fakeServerUrl, _maxRetry))
            {
                test.OnReconnectionAttempt += i => connectionAttempt = i;

                var taskResult = test.ReadAsync(4);

                await TaskTest.WaitFor(() => connectionAttempt > 1);

                test.Dispose();
                await Task.WhenAny(taskResult, Task.Delay(1000)).ConfigureAwait(false);

                Assert.That(taskResult.IsFaulted, Is.True);
                Assert.That(taskResult.Exception.InnerException, Is.TypeOf<ObjectDisposedException>());
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task KafkaTcpSocketShouldDisposeEvenWhileAwaitingReadAndThrowException()
        {
            using (var server = new FakeTcpServer(_log, FakeServerPort))
            using (var test = new KafkaTcpSocket(_log, _fakeServerUrl, _maxRetry))
            {
                int readSize = 0;

                test.OnReadFromSocketAttempt += i => readSize = i;

                var taskResult = test.ReadAsync(4);

                await TaskTest.WaitFor(() => readSize > 0);

                using (test) { }

                taskResult.ContinueWith(t => taskResult = t).Wait(TimeSpan.FromSeconds(1));

                Assert.That(taskResult.IsFaulted, Is.True);
                Assert.That(taskResult.Exception.InnerException, Is.TypeOf<ObjectDisposedException>());
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void KafkaTcpSocketShouldDisposeEvenWhileWriting()
        {
            using (var test = new KafkaTcpSocket(_log, _fakeServerUrl, _maxRetry))
            {
                int writeSize = 0;
                test.OnWriteToSocketAttempt += payload => writeSize = payload.Buffer.Length;

                var taskResult = test.WriteAsync(4.ToBytes().ToPayload());

                var wait = TaskTest.WaitFor(() => writeSize > 0);

                using (test)
                {
                } //allow the sockets to set

                taskResult.ContinueWith(t => taskResult = t).Wait(TimeSpan.FromSeconds(20));

                Assert.That(taskResult.IsCompleted, Is.True);
                Assert.That(taskResult.IsFaulted, Is.True, "Task should result indicate a fault.");
                Assert.That(taskResult.Exception.InnerException, Is.TypeOf<ObjectDisposedException>(),
                    "Exception should be a disposed exception.");
            }
        }

        #endregion Dispose Tests...

        #region Read Tests...

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void ReadShouldCancelWhileAwaitingResponse()
        {
            using (var server = new FakeTcpServer(_log, FakeServerPort))
            using (var test = new KafkaTcpSocket(_log, _fakeServerUrl, _maxRetry))
            {
                var count = 0;
                var semaphore = new SemaphoreSlim(0);
                var token = new CancellationTokenSource();

                test.ReadAsync(4, token.Token).ContinueWith(t =>
                    {
                        Interlocked.Increment(ref count);
                        Assert.That(t.IsCanceled, Is.True, "Task should be set to cancelled when disposed.");
                        semaphore.Release();
                    });

                Thread.Sleep(100);
                token.Cancel();

                semaphore.Wait(TimeSpan.FromSeconds(1));
                Assert.That(count, Is.EqualTo(1), "Read should have cancelled and incremented count.");
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task ReadShouldCancelWhileAwaitingReconnection()
        {
            int connectionAttempt = 0;
            using (var test = new KafkaTcpSocket(_log, _fakeServerUrl, _maxRetry))
            using (var token = new CancellationTokenSource())
            {
                test.OnReconnectionAttempt += i => connectionAttempt = i;

                var taskResult = test.ReadAsync(4, token.Token);

                await TaskTest.WaitFor(() => connectionAttempt > 1);

                token.Cancel();

                taskResult.SafeWait(TimeSpan.FromMilliseconds(1000));

                Assert.That(taskResult.IsCanceled, Is.True);
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task SocketShouldReconnectEvenAfterCancelledRead()
        {
            int connectionAttempt = 0;
            using (var test = new KafkaTcpSocket(_log, _fakeServerUrl, _maxRetry))
            using (var token = new CancellationTokenSource())
            {
                test.OnReconnectionAttempt += i => Interlocked.Exchange(ref connectionAttempt, i);

                var taskResult = test.ReadAsync(4, token.Token);

                await TaskTest.WaitFor(() => connectionAttempt > 1);

                var attemptsMadeSoFar = connectionAttempt;

                token.Cancel();

                await TaskTest.WaitFor(() => connectionAttempt > attemptsMadeSoFar);

                Assert.That(connectionAttempt, Is.GreaterThan(attemptsMadeSoFar));
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task ReadShouldBlockUntilAllBytesRequestedAreReceived()
        {
            using (var server = new FakeTcpServer(_log, FakeServerPort))
            using (var test = new KafkaTcpSocket(_log, _fakeServerUrl, _maxRetry))
            {
                var sendCompleted = 0;
                var bytesReceived = 0;

                test.OnBytesReceived += i => Interlocked.Add(ref bytesReceived, i);

                var resultTask = test.ReadAsync(4).ContinueWith(t =>
                    {
                        Interlocked.Increment(ref sendCompleted);
                        return t.Result;
                    });

                Console.WriteLine("Sending first 3 bytes...");
                server.HasClientConnected.Wait(TimeSpan.FromMilliseconds(1000));
                var sendInitialBytes = server.SendDataAsync(new byte[] { 0, 0, 0 }).Wait(TimeSpan.FromSeconds(10));
                Assert.That(sendInitialBytes, Is.True, "First 3 bytes should have been sent.");

                Console.WriteLine("Ensuring task blocks...");
                await TaskTest.WaitFor(() => bytesReceived > 0);
                Assert.That(resultTask.IsCompleted, Is.False, "Task should still be running, blocking.");
                Assert.That(sendCompleted, Is.EqualTo(0), "Should still block even though bytes have been received.");
                Assert.That(bytesReceived, Is.EqualTo(3), "Three bytes should have been received and we are waiting on the last byte.");

                Console.WriteLine("Sending last byte...");
                var sendLastByte = server.SendDataAsync(new byte[] { 0 }).Wait(TimeSpan.FromSeconds(10));
                Assert.That(sendLastByte, Is.True, "Last byte should have sent.");

                Console.WriteLine("Ensuring task unblocks...");
                await TaskTest.WaitFor(() => resultTask.IsCompleted);
                Assert.That(bytesReceived, Is.EqualTo(4), "Should have received 4 bytes.");
                Assert.That(resultTask.IsCompleted, Is.True, "Task should have completed.");
                Assert.That(sendCompleted, Is.EqualTo(1), "Task ContinueWith should have executed.");
                Assert.That(resultTask.Result.Length, Is.EqualTo(4), "Result of task should be 4 bytes.");
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void ReadShouldBeAbleToReceiveMoreThanOnce()
        {
            using (var server = new FakeTcpServer(_log, FakeServerPort))
            using (var test = new KafkaTcpSocket(_log, _fakeServerUrl, _maxRetry))
            {
                const int firstMessage = 99;
                const string secondMessage = "testmessage";

                Console.WriteLine("Sending first message to receive...");
                var send = server.SendDataAsync(firstMessage.ToBytes());

                var firstResponse = test.ReadAsync(4).Result.ToInt32();
                Assert.That(firstResponse, Is.EqualTo(firstMessage));

                Console.WriteLine("Sending second message to receive...");
                server.SendDataAsync(secondMessage);

                var secondResponse = Encoding.ASCII.GetString(test.ReadAsync(secondMessage.Length).Result);
                Assert.That(secondResponse, Is.EqualTo(secondMessage));
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void ReadShouldBeAbleToReceiveMoreThanOnceAsyncronously()
        {
            using (var server = new FakeTcpServer(_log, FakeServerPort))
            using (var test = new KafkaTcpSocket(_log, _fakeServerUrl, _maxRetry))
            {
                const int firstMessage = 99;
                const int secondMessage = 100;

                Console.WriteLine("Sending first message to receive...");
                var send1 = server.SendDataAsync(firstMessage.ToBytes());
                var firstResponseTask = test.ReadAsync(4);

                Console.WriteLine("Sending second message to receive...");
                var send2 = server.SendDataAsync(secondMessage.ToBytes());
                var secondResponseTask = test.ReadAsync(4);

                Assert.That(firstResponseTask.Result.ToInt32(), Is.EqualTo(firstMessage));
                Assert.That(secondResponseTask.Result.ToInt32(), Is.EqualTo(secondMessage));
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void ReadShouldNotLoseDataFromStreamOverMultipleReads()
        {
            using (var server = new FakeTcpServer(_log, FakeServerPort))
            using (var test = new KafkaTcpSocket(_log, _fakeServerUrl, _maxRetry))
            {
                const int firstMessage = 99;
                const string secondMessage = "testmessage";

                var payload = new KafkaMessagePacker()
                    .Pack(firstMessage)
                    .Pack(secondMessage, StringPrefixEncoding.None);

                //send the combined payload
                var send = server.SendDataAsync(payload.PayloadNoLength());

                var firstResponse = test.ReadAsync(4).Result.ToInt32();
                Assert.That(firstResponse, Is.EqualTo(firstMessage));

                var secondResponse = Encoding.ASCII.GetString(test.ReadAsync(secondMessage.Length).Result);
                Assert.That(secondResponse, Is.EqualTo(secondMessage));
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task ReadShouldThrowServerDisconnectedExceptionWhenDisconnected()
        {
            using (var server = new FakeTcpServer(_log, FakeServerPort))
            {
                var socket = new KafkaTcpSocket(_log, _fakeServerUrl, _maxRetry);

                var resultTask = socket.ReadAsync(4);

                //wait till connected
                await TaskTest.WaitFor(() => server.ConnectionEventcount > 0);

                server.DropConnection();

                await TaskTest.WaitFor(() => server.DisconnectionEventCount > 0);

                resultTask.ContinueWith(t => resultTask = t).Wait(TimeSpan.FromSeconds(1));

                Assert.That(resultTask.IsFaulted, Is.True);
                Assert.That(resultTask.Exception.InnerException, Is.TypeOf<KafkaConnectionException>());
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [ExpectedException(typeof(KafkaConnectionException))]
        public async Task WhenNoConnectionThrowSocketException()
        {
            var socket = new KafkaTcpSocket(_log, new KafkaEndpoint(new Uri("http://not.com"), null), _maxRetry);

            var resultTask = socket.ReadAsync(4);
            await resultTask;
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [ExpectedException(typeof(KafkaConnectionException))]
        public async Task WhenNoConnectionThrowSocketExceptionAfterMaxRetry()
        {
            var reconnectionAttempt = 0;
            var socket = new KafkaTcpSocket(_log, new KafkaEndpoint(new Uri("http://not.com"), null), _maxRetry);
            socket.OnReconnectionAttempt += (x) => Interlocked.Increment(ref reconnectionAttempt);
            var resultTask = socket.ReadAsync(4);
            await resultTask;
            Assert.Equals(reconnectionAttempt, _maxRetry);
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task ShouldReconnectAfterLosingConnectionAndBeAbleToStartNewRead()
        {
            using (var server = new FakeTcpServer(_log, FakeServerPort))
            {
                var disconnects = 0;
                var connects = 0;
                TaskCompletionSource<int> disconnectEvent = new TaskCompletionSource<int>();

                server.OnClientConnected += () => Interlocked.Increment(ref connects);
                server.OnClientDisconnected += () => Interlocked.Increment(ref disconnects);
                var socket = new KafkaTcpSocket(_log, _fakeServerUrl, _maxRetry);
                socket.OnServerDisconnected += () => disconnectEvent.TrySetResult(1);

                //wait till connected
                await TaskTest.WaitFor(() => connects > 0);
                var readTask = socket.ReadAsync(4);

                server.DropConnection();

                //wait till Disconnected
                await Task.WhenAny(disconnectEvent.Task, Task.Delay(100000));
                Assert.IsTrue(disconnectEvent.Task.IsCompleted, "Server should have disconnected the client.");
                Assert.Throws<KafkaConnectionException>(async () => await readTask);
                await TaskTest.WaitFor(() => connects == 2, 6000);
                Assert.That(connects, Is.EqualTo(2), "Socket should have reconnected.");

                var readTask2 = socket.ReadAsync(4);

                await server.SendDataAsync(99.ToBytes());
                var result = await readTask2;
                Assert.That(result.ToInt32(), Is.EqualTo(99), "Socket should have received the 4 bytes.");
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void ReadShouldStackReadRequestsAndReturnOneAtATime()
        {
            using (var server = new FakeTcpServer(_log, FakeServerPort))
            {
                var messages = new[] { "test1", "test2", "test3", "test4" };
                var expectedLength = "test1".Length;

                var payload = new KafkaMessagePacker().Pack(messages);

                var socket = new KafkaTcpSocket(new TraceLog(LogLevel.Warn), _fakeServerUrl, _maxRetry);

                var tasks = messages.Select(x => socket.ReadAsync(x.Length)).ToArray();

                var send = server.SendDataAsync(payload.Payload());

                Task.WaitAll(tasks);

                foreach (var task in tasks)
                {
                    Assert.That(task.Result.Length, Is.EqualTo(expectedLength));
                }
            }
        }

        #endregion Read Tests...

        #region Write Tests...

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task WriteAsyncShouldSendData()
        {
            using (var server = new FakeTcpServer(_log, FakeServerPort))
            using (var test = new KafkaTcpSocket(new TraceLog(LogLevel.Warn), _fakeServerUrl, _maxRetry))
            {
                const int testData = 99;
                int result = 0;

                server.OnBytesReceived += data => result = data.ToInt32();

                test.WriteAsync(testData.ToBytes().ToPayload()).Wait(TimeSpan.FromSeconds(1));
                await TaskTest.WaitFor(() => result > 0);
                Assert.That(result, Is.EqualTo(testData));
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task WriteAsyncShouldAllowMoreThanOneWrite()
        {
            using (var server = new FakeTcpServer(_log, FakeServerPort))
            using (var test = new KafkaTcpSocket(_log, _fakeServerUrl, _maxRetry))
            {
                const int testData = 99;
                var results = new List<byte>();

                server.OnBytesReceived += results.AddRange;

                Task.WaitAll(test.WriteAsync(testData.ToBytes().ToPayload()), test.WriteAsync(testData.ToBytes().ToPayload()));
                await TaskTest.WaitFor(() => results.Count >= 8);
                Assert.That(results.Count, Is.EqualTo(8));
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void WriteAndReadShouldBeAsynchronous()
        {
            //     for (int j = 0; j < 1000; j++)
            //     {
            var expected = new List<int> { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
            var write = new ConcurrentBag<int>();
            var read = new ConcurrentBag<int>();

            AutoResetEvent allEventAreArrived = new AutoResetEvent(false);
            AutoResetEvent allrReadEventAreArrived = new AutoResetEvent(false);
            using (var server = new FakeTcpServer(_log, FakeServerPort))
            using (var test = new KafkaTcpSocket(_log, _fakeServerUrl, _maxRetry))
            {
                server.OnBytesReceived += data =>
                {
                    var d = data.Batch(4).Select(x => x.ToArray().ToInt32());
                    foreach (var item in d)
                    {
                        write.Add(item);
                    }

                    if (expected.Count == write.Count)
                    {
                        allEventAreArrived.Set();
                    }
                };
                var tasks = Enumerable.Range(1, 10)
                    .SelectMany(i => new[]
                        {
                            test.WriteAsync(i.ToBytes().ToPayload()),
                            test.ReadAsync(4).ContinueWith(t =>
                            {
                                read.Add(t.Result.ToInt32());
                                if (read.Count == expected.Count)
                                {
                                    allrReadEventAreArrived.Set();
                                }
                            }),
                            server.SendDataAsync(i.ToBytes())
                        }).ToArray();

                Task.WaitAll(tasks);
                Assert.IsTrue(allEventAreArrived.WaitOne(3000), "not Get all write event in time");
                Assert.IsTrue(allrReadEventAreArrived.WaitOne(3000), "not Get all  read event in time");
                var w = write.OrderBy(x => x);
                var r = read.OrderBy(x => x);

                for (int i = 0; i < expected.Count; i++)
                {
                    //  _log.InfoFormat("write was {0}  expected {1}", w.ElementAt(i), expected[i]);
                    Assert.That(w.ElementAt(i), Is.EqualTo(expected[i]));
                }
                for (int i = 0; i < expected.Count; i++)
                {
                    //     _log.InfoFormat("read was {0}  expected {1}", r.ElementAt(i), expected[i]);
                    Assert.That(r.ElementAt(i), Is.EqualTo(expected[i]));
                }
            }
            //   }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void WriteShouldHandleLargeVolumeSendAsynchronously()
        {
            AutoResetEvent allEventAreArrived = new AutoResetEvent(false);
            var write = new ConcurrentBag<int>();
            int percent = 0;
            using (var server = new FakeTcpServer(_log, FakeServerPort))
            using (var test = new KafkaTcpSocket(new TraceLog(LogLevel.Warn), _fakeServerUrl, _maxRetry))
            {
                int numberOfWrite = 10000;
                server.OnBytesReceived += data =>
                {
                    var d = data.Batch(4).Select(x => x.ToArray().ToInt32());
                    foreach (var item in d)
                    {
                        write.Add(item);
                    }

                    if (write.Count % (numberOfWrite / 100) == 0)
                    {
                        Console.WriteLine("*************** done  percent:" + percent);
                        percent++;
                    }
                    if (write.Count == numberOfWrite)
                    {
                        allEventAreArrived.Set();
                    }
                };
                var tasks = Enumerable.Range(1, 10000).SelectMany(i => new[] { test.WriteAsync(i.ToBytes().ToPayload()), }).ToArray();

                Task.WaitAll(tasks);
                Assert.IsTrue(allEventAreArrived.WaitOne(2000), "not get all event on time");
                Assert.That(write.OrderBy(x => x), Is.EqualTo(Enumerable.Range(1, numberOfWrite)));
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task WriteShouldCancelWhileSendingData()
        {
            var writeAttempts = 0;
            using (var server = new FakeTcpServer(_log, FakeServerPort))
            using (var test = new KafkaTcpSocket(_log, _fakeServerUrl, _maxRetry))
            using (var token = new CancellationTokenSource())
            {
                test.OnWriteToSocketAttempt += payload => Interlocked.Increment(ref writeAttempts);

                var write = test.WriteAsync(new KafkaDataPayload { Buffer = 1.ToBytes() }, token.Token);

                await TaskTest.WaitFor(() => server.ConnectionEventcount > 0);
                await TaskTest.WaitFor(() => writeAttempts > 0);

                Assert.That(writeAttempts, Is.EqualTo(1), "Socket should have attempted to write.");

                //create a buffer write that will take a long time
                var data = Enumerable.Range(0, 100000000).Select(b => (byte)b).ToArray();
                token.Cancel();
                var taskResult = test.WriteAsync(
                        new KafkaDataPayload { Buffer = data },
                        token.Token);
                await Task.WhenAny(taskResult, Task.Delay(TimeSpan.FromSeconds(5))).ConfigureAwait(false);

                Assert.That(taskResult.IsCanceled, Is.True, "Task should have cancelled.");
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void WriteShouldCancelWhileAwaitingReconnection()
        {
            int connectionAttempt = 0;
            using (var test = new KafkaTcpSocket(_log, _fakeServerUrl, _maxRetry))
            using (var token = new CancellationTokenSource())
            {
                test.OnReconnectionAttempt += i => connectionAttempt = i;

                var taskResult = test.WriteAsync(new KafkaDataPayload { Buffer = 1.ToBytes() }, token.Token);

                var wait = TaskTest.WaitFor(() => connectionAttempt > 1);

                token.Cancel();

                taskResult.SafeWait(TimeSpan.FromMilliseconds(1000));

                Assert.That(taskResult.IsCanceled, Is.True);
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task SocketShouldReconnectEvenAfterCancelledWrite()
        {
            int connectionAttempt = 0;
            using (var test = new KafkaTcpSocket(_log, _fakeServerUrl, _maxRetry))
            using (var token = new CancellationTokenSource())
            {
                test.OnReconnectionAttempt += i => Interlocked.Exchange(ref connectionAttempt, i);

                var taskResult = test.WriteAsync(new KafkaDataPayload { Buffer = 1.ToBytes() }, token.Token);

                var wait = TaskTest.WaitFor(() => connectionAttempt > 1);

                var attemptsMadeSoFar = connectionAttempt;

                token.Cancel();

                await TaskTest.WaitFor(() => connectionAttempt > attemptsMadeSoFar);

                Assert.That(connectionAttempt, Is.GreaterThan(attemptsMadeSoFar));
            }
        }

        #endregion Write Tests...
    }
}