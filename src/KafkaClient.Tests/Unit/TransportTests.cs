using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
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
    [TestFixture]
    public class TransportTests
    {
        #region Connect

        [Test]
        public async Task ShouldAttemptMultipleTimesWhenConnectionFails()
        {
            var count = 0;
            var config = new ConnectionConfiguration(onConnecting: (e, a, _) => Interlocked.Increment(ref count));
            using (var transport = new SocketTransport(TestConfig.ServerEndpoint(), config, TestConfig.Log)) {
                var task = transport.ConnectAsync(CancellationToken.None);
                await TaskTest.WaitFor(() => count > 1, 10000);
                Assert.That(count, Is.GreaterThan(1));
            }
        }

        #endregion

        #region Dispose

        [Test]
        public async Task ShouldDisposeWithoutExceptionThrown()
        {
            var endpoint = TestConfig.ServerEndpoint();
            using (var server = new TcpServer(endpoint.Ip.Port, TestConfig.Log))
            {
                var transport = new SocketTransport(endpoint, TestConfig.Options.ConnectionConfiguration, TestConfig.Log);
                server.OnConnected = () => transport.Dispose();
                await Task.WhenAny(transport.ConnectAsync(CancellationToken.None), Task.Delay(TimeSpan.FromSeconds(3)));
                transport.Dispose();
            }
        }

        [Test]
        public async Task ShouldDisposeEvenWhilePollingToReconnect()
        {
            var connectionAttempt = -1;
            var config = new ConnectionConfiguration(Retry.AtMost(5), onConnecting: (e, a, _) => connectionAttempt = a);
            var endpoint = TestConfig.ServerEndpoint();
            using (var transport = new SocketTransport(endpoint, config, TestConfig.Log)) {
                var taskResult = transport.ConnectAsync(CancellationToken.None);

                await TaskTest.WaitFor(() => connectionAttempt >= 0);

                transport.Dispose();
                await Task.WhenAny(taskResult, Task.Delay(1000)).ConfigureAwait(false);

                Assert.That(taskResult.IsFaulted, Is.True);
                Assert.That(taskResult.Exception.InnerException, Is.TypeOf<ObjectDisposedException>());
            }
        }

        [Test]
        public async Task ShouldDisposeEvenWhileAwaitingReadAndThrowException()
        {
            int readSize = 0;
            var config = new ConnectionConfiguration(onReading: (e, size) => readSize = size);
            var endpoint = TestConfig.ServerEndpoint();
            using (new TcpServer(endpoint.Ip.Port, TestConfig.Log)) {
                var transport = new SocketTransport(endpoint, config, TestConfig.Log);
                try {
                    await transport.ConnectAsync(CancellationToken.None);
                    var buffer = new byte[4];
                    var taskResult = transport.ReadBytesAsync(buffer, 4, _ => { }, CancellationToken.None);

                    await TaskTest.WaitFor(() => readSize > 0);

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
            using (var server = new TcpServer(endpoint.Ip.Port, TestConfig.Log)) {
                using (var transport = new SocketTransport(endpoint, config, TestConfig.Log)) {
                    await transport.ConnectAsync(CancellationToken.None);
                    var buffer = new byte[4];
                    var readTask = transport.ReadBytesAsync(buffer, 4, _ => { }, CancellationToken.None);

                    // Sending first 3 bytes...
                    await Task.WhenAny(server.ClientConnected, Task.Delay(TimeSpan.FromSeconds(3)));
                    await server.SendDataAsync(new ArraySegment<byte>(new byte[] { 0, 0, 0 }));

                    // Ensuring task blocks...
                    await TaskTest.WaitFor(() => bytesReceived > 0);
                    Assert.That(readTask.IsCompleted, Is.False, "Task should still be running, blocking.");
                    Assert.That(readCompleted, Is.EqualTo(0), "Should still block even though bytes have been received.");
                    Assert.That(bytesReceived, Is.EqualTo(3), "Three bytes should have been received and we are waiting on the last byte.");

                    // Sending last byte...
                    var sendLastByte = await server.SendDataAsync(new ArraySegment<byte>(new byte[] { 0 }));
                    Assert.That(sendLastByte, Is.True, "Last byte should have sent.");

                    // Ensuring task unblocks...
                    await TaskTest.WaitFor(() => readTask.IsCompleted);
                    Assert.That(bytesReceived, Is.EqualTo(4), "Should have received 4 bytes.");
                    Assert.That(readTask.IsCompleted, Is.True, "Task should have completed.");
                    Assert.That(readCompleted, Is.EqualTo(1), "Task ContinueWith should have executed.");
                }
            }
        }

        [Test]
        public async Task ReadShouldBeAbleToReceiveMoreThanOnce()
        {
            var endpoint = TestConfig.ServerEndpoint();
            using (var server = new TcpServer(endpoint.Ip.Port, TestConfig.Log)) {
                using (var transport = new SocketTransport(endpoint, TestConfig.Options.ConnectionConfiguration, TestConfig.Log)) {
                    const int firstMessage = 99;
                    const string secondMessage = "testmessage";

                    // Sending first message to receive...
                    var send = server.SendDataAsync(new ArraySegment<byte>(firstMessage.ToBytes()));

                    await transport.ConnectAsync(CancellationToken.None);
                    var buffer = new byte[48];
                    await transport.ReadBytesAsync(buffer, 4, _ => { }, CancellationToken.None);
                    Assert.That(buffer.ToInt32(), Is.EqualTo(firstMessage));

                    // Sending second message to receive...
                    var send2 = (Task) server.SendDataAsync(new ArraySegment<byte>(Encoding.ASCII.GetBytes(secondMessage)));
                    var result = new MemoryStream();
                    await transport.ReadBytesAsync(buffer, secondMessage.Length, _ => { result.Write(buffer, 0, _); }, CancellationToken.None);
                    Assert.That(Encoding.ASCII.GetString(result.ToArray(), 0, (int)result.Position), Is.EqualTo(secondMessage));
                }
            }
        }

        [Test]
        public async Task ReadShouldBeAbleToReceiveMoreThanOnceAsyncronously()
        {
            var endpoint = TestConfig.ServerEndpoint();
            using (var server = new TcpServer(endpoint.Ip.Port, TestConfig.Log)) {
                using (var transport = new SocketTransport(endpoint, TestConfig.Options.ConnectionConfiguration, TestConfig.Log)) {
                    const int firstMessage = 99;
                    const int secondMessage = 100;

                    await transport.ConnectAsync(CancellationToken.None);

                    // Sending first message to receive..."
                    var send1 = server.SendDataAsync(new ArraySegment<byte>(firstMessage.ToBytes()));
                    var buffer1 = new byte[4];
                    var firstResponseTask = transport.ReadBytesAsync(buffer1, 4, _ => { }, CancellationToken.None);

                    // Sending second message to receive...
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
            using (var server = new TcpServer(endpoint.Ip.Port, TestConfig.Log)) {
                using (var transport = new SocketTransport(endpoint, TestConfig.Options.ConnectionConfiguration, TestConfig.Log)) {
                    const int firstMessage = 99;
                    const string secondMessage = "testmessage";
                    var bytes = Encoding.UTF8.GetBytes(secondMessage);

                    var payload = new KafkaWriter()
                        .Write(firstMessage);

                    //send the combined payload
                    var send = server.SendDataAsync(payload.ToSegment(false));

                    await transport.ConnectAsync(CancellationToken.None);
                    var buffer = new byte[48];
                    var read = await transport.ReadBytesAsync(buffer, 4, _ => { }, CancellationToken.None);
                    await send;
                    Assert.That(read, Is.EqualTo(4));
                    Assert.That(buffer.ToInt32(), Is.EqualTo(firstMessage));

                    // Sending second message to receive...
                    var send2 = server.SendDataAsync(new ArraySegment<byte>(Encoding.ASCII.GetBytes(secondMessage)));
                    var result = new MemoryStream();
                    await transport.ReadBytesAsync(buffer, secondMessage.Length, _ => { result.Write(buffer, 0, _); }, CancellationToken.None);
                    await send2;
                    Assert.That(Encoding.ASCII.GetString(result.ToArray(), 0, (int)result.Position), Is.EqualTo(secondMessage));
                }
            }
        }

        [Test]
        public async Task ReadShouldThrowServerDisconnectedExceptionWhenDisconnected()
        {
            var disconnectedCount = 0;
            var endpoint = TestConfig.ServerEndpoint();
            using (var server = new TcpServer(endpoint.Ip.Port, TestConfig.Log) {
                OnDisconnected = () => Interlocked.Increment(ref disconnectedCount)
            }) {
                using (var transport = new SocketTransport(endpoint, TestConfig.Options.ConnectionConfiguration, TestConfig.Log)) {
                    await transport.ConnectAsync(CancellationToken.None);
                    var buffer = new byte[48];
                    var taskResult = transport.ReadBytesAsync(buffer, 4, _ => { }, CancellationToken.None);

                    //wait till connected
                    await Task.WhenAny(server.ClientConnected, Task.Delay(TimeSpan.FromSeconds(3)));

                    server.DropConnection();

                    await TaskTest.WaitFor(() => disconnectedCount > 0);

                    await Task.WhenAny(taskResult, Task.Delay(1000)).ConfigureAwait(false);

                    Assert.That(taskResult.IsFaulted, Is.True);
                    Assert.That(taskResult.Exception.InnerException, Is.TypeOf<ConnectionException>());
                }
            }
        }

        [Test]
        public async Task WhenNoConnectionThrowSocketExceptionAfterMaxRetry()
        {
            var reconnectionAttempt = 0;
            const int maxAttempts = 3;
            var endpoint = TestConfig.ServerEndpoint();
            var config = new ConnectionConfiguration(
                Retry.AtMost(maxAttempts),
                onConnecting: (e, attempt, elapsed) => Interlocked.Increment(ref reconnectionAttempt)
                );
            using (var transport = new SocketTransport(endpoint, config, TestConfig.Log)) {
                try {
                    await transport.ConnectAsync(CancellationToken.None);
                    Assert.Fail("Did not throw ConnectionException");
                } catch (ConnectionException) {
                    // expected
                }
                Assert.That(reconnectionAttempt, Is.EqualTo(maxAttempts + 1));
            }
        }

        [Test]
        public async Task ReadShouldStackReadRequestsAndReturnOneAtATime()
        {
            var endpoint = TestConfig.ServerEndpoint();
            using (var server = new TcpServer(endpoint.Ip.Port, TestConfig.Log))
            {
                var messages = new[] { "test1", "test2", "test3", "test4" };
                var expectedLength = "test1".Length;

                var payload = new KafkaWriter().Write(messages);

                using (var transport = new SocketTransport(endpoint, TestConfig.Options.ConnectionConfiguration, TestConfig.Log)) {
                    await transport.ConnectAsync(CancellationToken.None);
                    var tasks = messages.Select(
                        x => {
                            var b = new byte[x.Length];
                            return transport.ReadBytesAsync(b, b.Length, _ => { }, CancellationToken.None);
                        }).ToArray();

                    var send = server.SendDataAsync(payload.ToSegment());

                    await Task.WhenAll(tasks);

                    foreach (var task in tasks) {
                        Assert.That(task.Result, Is.EqualTo(expectedLength));
                    }
                }
            }
        }

        #endregion


        [Test]
        public async Task WriteAsyncShouldSendData()
        {
            var endpoint = TestConfig.ServerEndpoint();
            using (var server = new TcpServer(endpoint.Ip.Port, TestConfig.Log))
            using (var transport = new SocketTransport(endpoint, TestConfig.Options.ConnectionConfiguration, TestConfig.Log))
            {
                const int testData = 99;
                int result = 0;

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
                server.OnReceivedAsync = async data => result = data.ToInt32();
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously

                await transport.ConnectAsync(CancellationToken.None);
                await transport.WriteBytesAsync(5, new ArraySegment<byte>(testData.ToBytes()), CancellationToken.None);
                await TaskTest.WaitFor(() => result > 0);
                Assert.That(result, Is.EqualTo(testData));
            }
        }

        [Test]
        public async Task WriteAsyncShouldAllowMoreThanOneWrite()
        {
            var endpoint = TestConfig.ServerEndpoint();
            using (var server = new TcpServer(endpoint.Ip.Port, TestConfig.Log))
            using (var transport = new SocketTransport(endpoint, TestConfig.Options.ConnectionConfiguration, TestConfig.Log))
            {
                const int testData = 99;
                var results = new List<byte>();

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
                server.OnReceivedAsync = async data => results.AddRange(data.Array.Skip(data.Offset).Take(data.Count));
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously

                await transport.ConnectAsync(CancellationToken.None);
                await Task.WhenAll(
                    transport.WriteBytesAsync(5, new ArraySegment<byte>(testData.ToBytes()), CancellationToken.None), 
                    transport.WriteBytesAsync(6, new ArraySegment<byte>(testData.ToBytes()), CancellationToken.None));
                await TaskTest.WaitFor(() => results.Count >= 8);
                Assert.That(results.Count, Is.EqualTo(8));
            }
        }

        [Test]
        public async Task AsynchronousWriteAndReadShouldBeConsistent()
        {
            const int requests = 10;
            var expected = requests.Repeat(i => i).ToList();
            var readOnServer = new ConcurrentBag<int>();
            var readOnClient = new ConcurrentBag<int>();

            var endpoint = TestConfig.ServerEndpoint();
            using (var server = new TcpServer(endpoint.Ip.Port, TestConfig.Log)) {
                using (var transport = new SocketTransport(endpoint, TestConfig.Options.ConnectionConfiguration, TestConfig.Log)) {
#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
                    server.OnReceivedAsync = async data => {
                        var d = data.Batch(4).Select(x => x.ToArray().ToInt32());
                        foreach (var item in d) {
                            readOnServer.Add(item);
                        }
                    };
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
                    await transport.ConnectAsync(CancellationToken.None);

                    var clientWriteTasks = expected.Select(i => transport.WriteBytesAsync(i, new ArraySegment<byte>(i.ToBytes()), CancellationToken.None));
                    var clientReadTasks = expected.Select(
                        i => {
                            var b = new byte[4];
                            return transport.ReadBytesAsync(b, b.Length, _ => { }, CancellationToken.None)
                                            .ContinueWith(t => readOnClient.Add(b.ToInt32()));
                        });
                    var serverWriteTasks = expected.Select(i => server.SendDataAsync(new ArraySegment<byte>(i.ToBytes())));

                    await Task.WhenAll(clientWriteTasks.Union(clientReadTasks).Union(serverWriteTasks));
                    await TaskTest.WaitFor(() => readOnServer.Count == requests);
                    Assert.That(readOnServer.Count, Is.EqualTo(requests), "not all writes propagated to the server in time");
                    await TaskTest.WaitFor(() => readOnClient.Count == requests);
                    Assert.That(readOnClient.Count, Is.EqualTo(requests), "not all reads happend on the client in time");
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
            var readOnServer = new ConcurrentBag<int>();
            var endpoint = TestConfig.ServerEndpoint();
            using (var server = new TcpServer(endpoint.Ip.Port, TestConfig.Log)) {
                using (var transport = new SocketTransport(endpoint, TestConfig.Options.ConnectionConfiguration, TestConfig.Log)) {
#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
                    server.OnReceivedAsync = async data =>
                    {
                        var d = data.Batch(4).Select(x => x.ToArray().ToInt32());
                        foreach (var item in d) {
                            readOnServer.Add(item);
                        }
                    };
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously

                    await transport.ConnectAsync(CancellationToken.None);
                    var clientWriteTasks = Enumerable.Range(1, requests).Select(i => transport.WriteBytesAsync(i, new ArraySegment<byte>(i.ToBytes()), CancellationToken.None));

                    await Task.WhenAll(clientWriteTasks);
                    await TaskTest.WaitFor(() => readOnServer.Count == requests);
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
            using (var server = new TcpServer(endpoint.Ip.Port, TestConfig.Log)) {
                using (var transport = new SocketTransport(endpoint, config, TestConfig.Log)) {
                    using (var token = new CancellationTokenSource())
                    {
                        await transport.ConnectAsync(token.Token);
                        var write = transport.WriteBytesAsync(5, new ArraySegment<byte>(1.ToBytes()), token.Token);

                        await Task.WhenAny(server.ClientConnected, Task.Delay(TimeSpan.FromSeconds(3)));
                        await TaskTest.WaitFor(() => clientWriteAttempts > 0);

                        Assert.That(clientWriteAttempts, Is.EqualTo(1), "Socket should have attempted to write.");

                        //create a buffer write that will take a long time
                        var data = Enumerable.Range(0, 100000000).Select(b => (byte)b).ToArray();
                        token.Cancel();
                        var taskResult = transport.WriteBytesAsync(6, new ArraySegment<byte>(data), token.Token);
                        await Task.WhenAny(taskResult, Task.Delay(TimeSpan.FromSeconds(5))).ConfigureAwait(false);

                        Assert.That(taskResult.IsCanceled || !taskResult.IsFaulted, Is.True, "Task should have cancelled.");
                    }
                }
            }
        }

        #region WriteBytes


        #endregion
    }
}