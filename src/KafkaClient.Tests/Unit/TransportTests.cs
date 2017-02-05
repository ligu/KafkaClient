using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Connections;
using KafkaClient.Protocol;
using KafkaClient.Testing;
using NUnit.Framework;

namespace KafkaClient.Tests.Unit
{
    public abstract class TransportTests<T> where T : class, ITransport
    {
        protected abstract T CreateTransport(Endpoint endpoint, IConnectionConfiguration configuration, ILog log);

        protected virtual TcpServer CreateServer(int port, ILog log) => new TcpServer(port, log);

        #region Connect

        [Test]
        public async Task ShouldAttemptMultipleTimesWhenConnectionFails()
        {
            var count = 0;
            var config = new ConnectionConfiguration(onConnecting: (e, a, _) => Interlocked.Increment(ref count));
            using (var transport = CreateTransport(TestConfig.ServerEndpoint(), config, TestConfig.Log)) {
                var task = transport.ConnectAsync(CancellationToken.None);
                await AssertAsync.ThatEventually(() => count > 1, TimeSpan.FromSeconds(10), () => $"count {count}");
            }
        }

        #endregion

        #region Dispose

        [Test]
        public async Task ShouldDisposeWithoutExceptionThrown()
        {
            var endpoint = TestConfig.ServerEndpoint();
            using (var server = CreateServer(endpoint.Ip.Port, TestConfig.Log))
            {
                var transport = CreateTransport(endpoint, TestConfig.Options.ConnectionConfiguration, TestConfig.Log);
                server.OnConnected = () => transport.Dispose();
                await Task.WhenAny(transport.ConnectAsync(CancellationToken.None), Task.Delay(TimeSpan.FromSeconds(3)));
                transport.Dispose();
            }
        }

        [Test]
        public async Task ShouldDisposeEvenWhilePollingToReconnect()
        {
            var connectionAttempt = -1;
            var config = new ConnectionConfiguration(Retry.Until(TimeSpan.FromSeconds(10)), onConnecting: (e, a, _) => connectionAttempt = a);
            var endpoint = TestConfig.ServerEndpoint();
            using (var transport = CreateTransport(endpoint, config, TestConfig.Log)) {
                var taskResult = transport.ConnectAsync(CancellationToken.None);

                await AssertAsync.ThatEventually(() => connectionAttempt >= 0, () => $"attempt {connectionAttempt}");
                transport.Dispose();

                using (var cancellation = new TimedCancellation(CancellationToken.None, TimeSpan.FromSeconds(3))) {
                    await AssertAsync.Throws<ObjectDisposedException>(() => taskResult.ThrowIfCancellationRequested(cancellation.Token));
                }
            }
        }

        [Test]
        public async Task ShouldDisposeEvenWhileAwaitingReadAndThrowException()
        {
            int readSize = 0;
            var config = new ConnectionConfiguration(onReading: (e, size) => readSize = size);
            var endpoint = TestConfig.ServerEndpoint();
            using (CreateServer(endpoint.Ip.Port, TestConfig.Log)) {
                var transport = CreateTransport(endpoint, config, TestConfig.Log);
                try {
                    await transport.ConnectAsync(CancellationToken.None);
                    var buffer = new byte[4];
                    var taskResult = transport.ReadBytesAsync(buffer, 4, _ => { }, CancellationToken.None);

                    await AssertAsync.ThatEventually(() => readSize > 0, () => $"readSize {readSize}");

                    using (transport) { }
                    transport = null;

                    await taskResult.CancelAfter();
                    Assert.Fail("Expected ObjectDisposedException to be thrown");
                } catch (ObjectDisposedException) {
                    // expected
                } finally {
                    transport?.Dispose();
                }
            }
        }

        #endregion

        #region ReadBytes

        [Test]
        public async Task ReadShouldBlockUntilAllBytesRequestedAreReceived()
        {
            var readCompleted = 0;
            var bytesReceived = 0;
            var config = new ConnectionConfiguration(
                onReadBytes: (e, attempted, actual, elapsed) => Interlocked.Add(ref bytesReceived, actual),
                onRead: (e, read, elapsed) => Interlocked.Increment(ref readCompleted));
            var endpoint = TestConfig.ServerEndpoint();
            using (var server = CreateServer(endpoint.Ip.Port, TestConfig.Log)) {
                using (var transport = CreateTransport(endpoint, config, TestConfig.Log)) {
                    await transport.ConnectAsync(CancellationToken.None);
                    var buffer = new byte[4];
                    var readTask = transport.ReadBytesAsync(buffer, 4, _ => { }, CancellationToken.None);

                    TestConfig.Log.Info(() => LogEvent.Create("Sending the first 3 bytes"));
                    await Task.WhenAny(server.ClientConnected, Task.Delay(TimeSpan.FromSeconds(3)));
                    await server.SendDataAsync(new ArraySegment<byte>(new byte[] { 0, 0, 0 }));

                    // Three bytes should have been received and we are waiting on the last byte.
                    await AssertAsync.ThatEventually(() => bytesReceived >= 3, () => $"bytesReceived {bytesReceived}");
                    Assert.That(readTask.IsCompleted, Is.False, "Task should still be running, blocking.");
                    Assert.That(readCompleted, Is.EqualTo(0), "Should still block even though bytes have been received.");

                    TestConfig.Log.Info(() => LogEvent.Create("Sending the last bytes"));
                    var sendLastByte = await server.SendDataAsync(new ArraySegment<byte>(new byte[] { 0 }));
                    Assert.That(sendLastByte, Is.True, "Last byte should have sent.");

                    // Ensuring task unblocks...
                    await AssertAsync.ThatEventually(() => readTask.IsCompleted);
                    Assert.That(bytesReceived, Is.EqualTo(4), "");
                    Assert.That(readCompleted, Is.EqualTo(1), "Task ContinueWith should have executed.");
                }
            }
        }

        [Test]
        public async Task ReadShouldBeAbleToReceiveMoreThanOnce()
        {
            var endpoint = TestConfig.ServerEndpoint();
            using (var server = CreateServer(endpoint.Ip.Port, TestConfig.Log)) {
                using (var transport = CreateTransport(endpoint, TestConfig.Options.ConnectionConfiguration, TestConfig.Log)) {
                    const int firstMessage = 99;
                    const string secondMessage = "testmessage";

                    await transport.ConnectAsync(CancellationToken.None);

                    TestConfig.Log.Info(() => LogEvent.Create("Sending first message to receive"));
                    var send = server.SendDataAsync(new ArraySegment<byte>(firstMessage.ToBytes()));

                    var buffer = new byte[48];
                    await transport.ReadBytesAsync(buffer, 4, _ => { }, CancellationToken.None);
                    Assert.That(buffer.ToInt32(), Is.EqualTo(firstMessage));

                    TestConfig.Log.Info(() => LogEvent.Create("Sending second message to receive"));
                    var send2 = (Task) server.SendDataAsync(new ArraySegment<byte>(Encoding.ASCII.GetBytes(secondMessage)));
                    var read = await transport.ReadBytesAsync(buffer, secondMessage.Length, _ => { }, CancellationToken.None);
                    Assert.That(Encoding.ASCII.GetString(buffer, 0, read), Is.EqualTo(secondMessage));
                }
            }
        }

        [Test]
        public async Task ReadShouldBeAbleToReceiveMoreThanOnceAsyncronously()
        {
            var endpoint = TestConfig.ServerEndpoint();
            using (var server = CreateServer(endpoint.Ip.Port, TestConfig.Log)) {
                using (var transport = CreateTransport(endpoint, TestConfig.Options.ConnectionConfiguration, TestConfig.Log)) {
                    const int firstMessage = 99;
                    const int secondMessage = 100;

                    await transport.ConnectAsync(CancellationToken.None);

                    TestConfig.Log.Info(() => LogEvent.Create("Sending first message to receive"));
                    var send1 = server.SendDataAsync(new ArraySegment<byte>(firstMessage.ToBytes()));
                    var buffer1 = new byte[4];
                    var firstResponseTask = transport.ReadBytesAsync(buffer1, 4, _ => { }, CancellationToken.None);

                    TestConfig.Log.Info(() => LogEvent.Create("Sending second message to receive"));
                    var send2 = server.SendDataAsync(new ArraySegment<byte>(secondMessage.ToBytes()));
                    var buffer2 = new byte[4];
                    var secondResponseTask = transport.ReadBytesAsync(buffer2, 4, _ => { }, CancellationToken.None);

                    await Task.WhenAll(firstResponseTask, secondResponseTask);
                    Assert.That(buffer1.ToInt32(), Is.EqualTo(firstMessage));
                    Assert.That(buffer2.ToInt32(), Is.EqualTo(secondMessage));
                }
            }
        }

        [Test]
        public async Task ReadShouldNotLoseDataFromStreamOverMultipleReads()
        {
            var endpoint = TestConfig.ServerEndpoint();
            using (var server = CreateServer(endpoint.Ip.Port, TestConfig.Log)) {
                using (var transport = CreateTransport(endpoint, TestConfig.Options.ConnectionConfiguration, TestConfig.Log)) {
                    const int firstMessage = 99;
                    const string secondMessage = "testmessage";

                    await transport.ConnectAsync(CancellationToken.None);

                    TestConfig.Log.Info(() => LogEvent.Create("Sending first message to receive"));
                    var send = server.SendDataAsync(new KafkaWriter().Write(firstMessage).ToSegment(false));

                    var buffer = new byte[96];
                    var read = await transport.ReadBytesAsync(buffer, 4, _ => { }, CancellationToken.None);
                    await send;
                    Assert.That(read, Is.EqualTo(4));
                    Assert.That(buffer.ToInt32(), Is.EqualTo(firstMessage));

                    TestConfig.Log.Info(() => LogEvent.Create("Sending second message to receive"));
                    var send2 = server.SendDataAsync(new ArraySegment<byte>(Encoding.UTF8.GetBytes(secondMessage)));
                    var receive2 = await transport.ReadBytesAsync(buffer, secondMessage.Length, _ => { }, CancellationToken.None);
                    await send2;
                    Assert.That(Encoding.ASCII.GetString(buffer, 0, receive2), Is.EqualTo(secondMessage));
                }
            }
        }

        [Test]
        public async Task ReadShouldThrowServerDisconnectedExceptionWhenDisconnected()
        {
            var disconnectedCount = 0;
            var endpoint = TestConfig.ServerEndpoint();
            using (var server = CreateServer(endpoint.Ip.Port, TestConfig.Log)) {
                server.OnDisconnected = () => Interlocked.Increment(ref disconnectedCount);
                using (var transport = CreateTransport(endpoint, TestConfig.Options.ConnectionConfiguration, TestConfig.Log)) {
                    await transport.ConnectAsync(CancellationToken.None);
                    var buffer = new byte[48];
                    var taskResult = transport.ReadBytesAsync(buffer, 4, _ => { }, CancellationToken.None);

                    //wait till connected
                    await Task.WhenAny(server.ClientConnected, Task.Delay(TimeSpan.FromSeconds(3)));

                    server.DropConnection();

                    await AssertAsync.ThatEventually(() => disconnectedCount > 0, () => $"disconnected {disconnectedCount}");

                    await Task.WhenAny(taskResult, Task.Delay(1000)).ConfigureAwait(false);

                    Assert.That(taskResult.IsFaulted, Is.True);
                    Assert.That(taskResult.Exception.InnerException, Is.TypeOf<ConnectionException>());
                }
            }
        }

        [Test]
        public async Task WhenNoConnectionThrowSocketExceptionAfterMaxRetry()
        {
            var connectionAttempts = 0;
            const int maxRetries = 2;
            var endpoint = TestConfig.ServerEndpoint();
            var config = new ConnectionConfiguration(
                Retry.AtMost(maxRetries),
                onConnecting: (e, attempt, elapsed) => Interlocked.Increment(ref connectionAttempts)
                );
            using (var transport = CreateTransport(endpoint, config, TestConfig.Log)) {
                try {
                    await transport.ConnectAsync(CancellationToken.None);
                    Assert.Fail("Did not throw ConnectionException");
                } catch (ConnectionException) {
                    // expected
                }
                Assert.That(connectionAttempts, Is.EqualTo(1 + maxRetries));
            }
        }

        [Test]
        public async Task ReadShouldStackReadRequestsAndReturnOneAtATime()
        {
            var endpoint = TestConfig.ServerEndpoint();
            using (var server = CreateServer(endpoint.Ip.Port, TestConfig.Log))
            {
                var messages = new[] { "test1", "test2", "test3", "test4" };
                var expectedLength = "test1".Length;

                using (var transport = CreateTransport(endpoint, TestConfig.Options.ConnectionConfiguration, TestConfig.Log)) {
                    await transport.ConnectAsync(CancellationToken.None);

                    var tasks = messages.Select(
                        message => {
                            var b = new byte[message.Length];
                            return transport.ReadBytesAsync(b, b.Length, _ => { }, CancellationToken.None);
                        }).ToArray();

                    var send = server.SendDataAsync(new KafkaWriter().Write(messages).ToSegment());

                    await Task.WhenAll(tasks);

                    foreach (var task in tasks) {
                        Assert.That(task.Result, Is.EqualTo(expectedLength));
                    }
                }
            }
        }

        #endregion

        #region WriteBytes

        [Test]
        public async Task WriteAsyncShouldSendData()
        {
            var endpoint = TestConfig.ServerEndpoint();
            using (var server = CreateServer(endpoint.Ip.Port, TestConfig.Log))
            using (var transport = CreateTransport(endpoint, TestConfig.Options.ConnectionConfiguration, TestConfig.Log))
            {
                const int testData = 99;
                var bytes = new byte[4];
                int read = 0;

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
                server.OnReceivedAsync = async data =>
                {
                    var offset = Interlocked.Add(ref read, data.Count) - data.Count;
                    for (var i = 0; i < data.Count; i++) {
                        bytes[offset + i] = data.Array[data.Offset + i];
                    }
                };
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously

                await transport.ConnectAsync(CancellationToken.None);
                await transport.WriteBytesAsync(new ArraySegment<byte>(testData.ToBytes()), CancellationToken.None);
                await AssertAsync.ThatEventually(() => read == 4 && bytes.ToInt32() == testData, () => $"read {read}");
            }
        }

        [Test]
        public async Task WriteAsyncShouldAllowMoreThanOneWrite()
        {
            var endpoint = TestConfig.ServerEndpoint();
            using (var server = CreateServer(endpoint.Ip.Port, TestConfig.Log))
            using (var transport = CreateTransport(endpoint, TestConfig.Options.ConnectionConfiguration, TestConfig.Log))
            {
                const int testData = 99;
                var results = new List<byte>();

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
                server.OnReceivedAsync = async data => results.AddRange(data.Array.Skip(data.Offset).Take(data.Count));
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously

                await transport.ConnectAsync(CancellationToken.None);
                await Task.WhenAll(
                    transport.WriteBytesAsync(new ArraySegment<byte>(testData.ToBytes()), CancellationToken.None), 
                    transport.WriteBytesAsync(new ArraySegment<byte>(testData.ToBytes()), CancellationToken.None));
                await AssertAsync.ThatEventually(() => results.Count == 8, () => $"count {results.Count}");
            }
        }

        [Test]
        public async Task AsynchronousWriteAndReadShouldBeConsistent()
        {
            const int requests = 10;
            var expected = requests.Repeat(i => i).ToList();
            var readOnClient = new ConcurrentBag<int>();

            var endpoint = TestConfig.ServerEndpoint();
            using (var server = CreateServer(endpoint.Ip.Port, TestConfig.Log)) {
                using (var transport = CreateTransport(endpoint, TestConfig.Options.ConnectionConfiguration, TestConfig.Log)) {
                    var bytes = new byte[4 * requests];
                    int bytesReadOnServer = 0;
                    server.OnReceivedAsync = data => {
                        var offset = Interlocked.Add(ref bytesReadOnServer, data.Count) - data.Count;
                        for (var i = 0; i < data.Count; i++) {
                            bytes[offset + i] = data.Array[data.Offset + i];
                        }
                        return Task.FromResult(0);
                    };
                    await transport.ConnectAsync(CancellationToken.None);

                    var clientWriteTasks = expected.Select(i => transport.WriteBytesAsync(new ArraySegment<byte>(i.ToBytes()), CancellationToken.None));
                    var clientReadTasks = expected.Select(
                        i => {
                            var b = new byte[4];
                            return transport.ReadBytesAsync(b, b.Length, _ => { }, CancellationToken.None)
                                            .ContinueWith(t => readOnClient.Add(b.ToInt32()));
                        });
                    var serverWriteTasks = expected.Select(i => server.SendDataAsync(new ArraySegment<byte>(i.ToBytes())));

                    await Task.WhenAll(clientWriteTasks.Union(clientReadTasks).Union(serverWriteTasks));

                    await AssertAsync.ThatEventually(() => bytesReadOnServer == bytes.Length, () => $"length {bytes.Length}, read {bytesReadOnServer}");
                    var readOnServer = new List<int>();
                    foreach (var value in bytes.Batch(4).Select(x => x.ToArray().ToInt32())) {
                        readOnServer.Add(value);
                    }
                    Assert.That(readOnServer.Count, Is.EqualTo(requests), "not all writes propagated to the server in time");
                    await AssertAsync.ThatEventually(() => readOnClient.Count == requests, () => $"read {readOnClient.Count}, requests {requests}");
                    var w = readOnServer.OrderBy(x => x);
                    var r = readOnClient.OrderBy(x => x);

                    for (var i = 0; i < requests; i++) {
                        Assert.That(w.ElementAt(i), Is.EqualTo(expected[i]));
                    }
                    for (var i = 0; i < requests; i++) {
                        Assert.That(r.ElementAt(i), Is.EqualTo(expected[i]));
                    }
                }
            }
        }

        [Test]
        public async Task WriteShouldHandleLargeVolumeSendAsynchronously([Values(1000, 5000)] int requests)
        {
            var endpoint = TestConfig.ServerEndpoint();
            using (var server = CreateServer(endpoint.Ip.Port, TestConfig.Log)) {
                using (var transport = CreateTransport(endpoint, TestConfig.Options.ConnectionConfiguration, TestConfig.Log)) {
                    var bytes = new byte[4 * requests];
                    int bytesReadOnServer = 0;

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
                    server.OnReceivedAsync = async data =>
                    {
                        var offset = Interlocked.Add(ref bytesReadOnServer, data.Count) - data.Count;
                        for (var i = 0; i < data.Count; i++) {
                            bytes[offset + i] = data.Array[data.Offset + i];
                        }
                    };
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously

                    await transport.ConnectAsync(CancellationToken.None);
                    var clientWriteTasks = Enumerable.Range(1, requests).Select(i => transport.WriteBytesAsync(new ArraySegment<byte>(i.ToBytes()), CancellationToken.None));

                    await Task.WhenAll(clientWriteTasks);
                    await AssertAsync.ThatEventually(() => bytesReadOnServer == bytes.Length, () => $"bytes read {bytesReadOnServer}, total {bytes.Length}");
                    var readOnServer = new List<int>();
                    foreach (var value in bytes.Batch(4).Select(x => x.ToArray().ToInt32())) {
                        readOnServer.Add(value);
                    }
                    Assert.That(readOnServer.Count, Is.EqualTo(requests), "not all writes propagated to the server in time");
                    Assert.That(readOnServer.OrderBy(x => x), Is.EqualTo(Enumerable.Range(1, requests)));
                }
            }
        }

        [Test]
        public async Task WriteShouldCancelWhileSendingData()
        {
            var clientWriteAttempts = 0;
            var config = new ConnectionConfiguration(onWritingBytes: (e, payload) => Interlocked.Increment(ref clientWriteAttempts));
            var endpoint = TestConfig.ServerEndpoint();
            using (var server = CreateServer(endpoint.Ip.Port, TestConfig.Log)) {
                using (var transport = CreateTransport(endpoint, config, TestConfig.Log)) {
                    using (var token = new CancellationTokenSource())
                    {
                        await transport.ConnectAsync(token.Token);
                        var write = transport.WriteBytesAsync(new ArraySegment<byte>(1.ToBytes()), token.Token);

                        await Task.WhenAny(server.ClientConnected, Task.Delay(TimeSpan.FromSeconds(3)));
                        await AssertAsync.ThatEventually(() => clientWriteAttempts == 1, () => $"attempts {clientWriteAttempts}");

                        //create a buffer write that will take a long time
                        var data = Enumerable.Range(0, 1000000).Select(b => (byte)b).ToArray();
                        token.Cancel();
                        var taskResult = transport.WriteBytesAsync(new ArraySegment<byte>(data), token.Token, 6);
                        await Task.WhenAny(taskResult, Task.Delay(TimeSpan.FromSeconds(2))).ConfigureAwait(false);

                        Assert.That(taskResult.IsCanceled || !taskResult.IsFaulted, Is.True, "Task should have cancelled.");
                    }
                }
            }
        }

        #endregion
    }
}