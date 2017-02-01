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
using Nito.AsyncEx;
using NUnit.Framework;

namespace KafkaClient.Tests.Unit
{
    [TestFixture]
    public class ConnectionTests
    {
        #region Construct

        [Test]
        public async Task ShouldStartReadPollingOnConstruction()
        {
            var endpoint = await Endpoint.ResolveAsync(TestConfig.ServerUri(), TestConfig.Log);
            using (var conn = new Connection(endpoint, log: TestConfig.Log))
            {
                await TaskTest.WaitFor(() => conn.IsReaderAlive);
                Assert.That(conn.IsReaderAlive, Is.True);
            }
        }

        [Test]
        public async Task ShouldReportServerUriOnConstruction()
        {
            var endpoint = await Endpoint.ResolveAsync(TestConfig.ServerUri(), TestConfig.Log);
            using (var conn = new Connection(endpoint, log: TestConfig.Log))
            {
                Assert.That(conn.Endpoint, Is.EqualTo(endpoint));
            }
        }

        [Test]
        public async Task ThrowsConnectionExceptionOnInvalidEndpoint()
        {
            var conn = new Connection(new Endpoint(null, "not.com"));
            try {
                await conn.UsingAsync(
                    async () => await conn.SendAsync(new ApiVersionsRequest(), CancellationToken.None));
                Assert.Fail("should have thrown ConnectionException");
            } catch (ConnectionException) {
                // expected
            }
        }

        #endregion 

        #region Connection

        [Test]
        public async Task ShouldStartDedicatedThreadOnConstruction()
        {
            var count = 0;
            var config = new ConnectionConfiguration(onConnecting: (e, a, _) => Interlocked.Increment(ref count));
            var endpoint = await Endpoint.ResolveAsync(TestConfig.ServerUri(), TestConfig.Log);
            using (var conn = new Connection(endpoint, config, log: TestConfig.Log))
            {
                await TaskTest.WaitFor(() => count > 0);
                Assert.That(count, Is.GreaterThan(0));
            }
        }

        [Test]
        public async Task ShouldAttemptMultipleTimesWhenConnectionFails()
        {
            var count = 0;
            var config = new ConnectionConfiguration(onConnecting: (e, a, _) => Interlocked.Increment(ref count));
            using (var conn = new Connection(await Endpoint.ResolveAsync(TestConfig.ServerUri(), TestConfig.Log), config, TestConfig.Log))
            {
                var task = conn.SendAsync(new FetchRequest(), CancellationToken.None); //will force a connection
                await TaskTest.WaitFor(() => count > 1, 10000);
                Assert.That(count, Is.GreaterThan(1));
            }
        }

        #endregion

        #region Dispose

        [Test]
        public async Task ShouldDisposeWithoutExceptionThrown()
        {
            var endpoint = await Endpoint.ResolveAsync(TestConfig.ServerUri(), TestConfig.Log);
            using (var server = new TcpServer(endpoint.Ip.Port, TestConfig.Log))
            {
                var conn = new Connection(endpoint, log: TestConfig.Log);
                await Task.WhenAny(server.ClientConnected, Task.Delay(TimeSpan.FromSeconds(3)));
                using (conn) { }
            }
        }

        [Test]
        public async Task ShouldDisposeWithoutExceptionEvenWhileCallingSendAsync()
        {
            var endpoint = await Endpoint.ResolveAsync(TestConfig.ServerUri(), TestConfig.Log);
            using (var conn = new Connection(endpoint, log: TestConfig.Log))
            {
                var task = conn.SendAsync(new MetadataRequest(), CancellationToken.None);
                await Task.WhenAny(task, Task.Delay(1000)).ConfigureAwait(false);
                Assert.That(task.IsCompleted, Is.False, "The send task should still be pending.");
            }
        }

        #endregion

        #region Read

        [Test]
        public async Task ShouldLogDisconnectAndRecover([Values(3, 4)] int connectionAttempts)
        {
            var mockLog = new MemoryLog();
            var clientDisconnected = 0;
            var clientConnected = 0;
            var serverConnected = 0;

            var config = new ConnectionConfiguration(
                onDisconnected: (e, exception) => {
                    Interlocked.Increment(ref clientDisconnected);
                },
                onConnected: (e, attempt, elapsed) => {
                    Interlocked.Increment(ref clientConnected);
                });

            var endpoint = await Endpoint.ResolveAsync(TestConfig.ServerUri(), TestConfig.Log);
            using (var server = new TcpServer(endpoint.Ip.Port, TestConfig.Log) {
                OnConnected = () => Interlocked.Increment(ref serverConnected)
            })
            using (new Connection(endpoint, config, log: mockLog))
            {
                for (var connectionAttempt = 1; connectionAttempt <= connectionAttempts; connectionAttempt++)
                {
                    var currentAttempt = connectionAttempt;
                    await TaskTest.WaitFor(() => serverConnected == currentAttempt);
                    Assert.That(serverConnected, Is.EqualTo(connectionAttempt));
                    await server.SendDataAsync(new ArraySegment<byte>(CreateCorrelationMessage(connectionAttempt)));
                    TestConfig.Log.Write(LogLevel.Info, () => LogEvent.Create($"Sent CONNECTION attempt {connectionAttempt}"));
                    await TaskTest.WaitFor(() => clientConnected == clientDisconnected, 200);

                    Assert.That(mockLog.LogEvents.Count(e => e.Item1 == LogLevel.Info && e.Item2.Message.StartsWith("Polling receive thread has recovered on ")), Is.EqualTo(currentAttempt-1));

                    TestConfig.Log.Write(LogLevel.Info, () => LogEvent.Create($"Dropping CONNECTION attempt {connectionAttempt}"));
                    server.DropConnection();
                    await TaskTest.WaitFor(() => clientDisconnected == currentAttempt);

                    Assert.That(mockLog.LogEvents.Count(e => e.Item1 == LogLevel.Info && e.Item2.Message.StartsWith("Disposing transport to")), Is.AtLeast(currentAttempt));
                }
            }
        }

        [Test]
        public async Task ShouldFinishPartiallyReadMessage()
        {
            var mockLog = new MemoryLog();
            var bytesRead = 0;

            var config = new ConnectionConfiguration(onReadBytes: (e, attempted, actual, elapsed) => Interlocked.Add(ref bytesRead, actual));

            var endpoint = await Endpoint.ResolveAsync(TestConfig.ServerUri(), TestConfig.Log);
            using (var server = new TcpServer(endpoint.Ip.Port, TestConfig.Log))
            using (new Connection(endpoint, config, mockLog))
            {
                // send size
                var size = 200;
                await server.SendDataAsync(new ArraySegment<byte>(size.ToBytes()));
                var random = new Random(42);
                var firstBytes = new byte[99];
                random.NextBytes(firstBytes);
                var offset = 0;
                var correlationId = 200;
                foreach (var b in correlationId.ToBytes()) {
                    firstBytes[offset++] = b;
                }

                // send half of payload
                await server.SendDataAsync(new ArraySegment<byte>(firstBytes));
                await TaskTest.WaitFor(() => bytesRead == firstBytes.Length);

                Assert.That(mockLog.LogEvents.Count(e => e.Item1 == LogLevel.Warn && e.Item2.Message.StartsWith($"Unexpected response (id {correlationId}, {size}? bytes) from")), Is.EqualTo(1));
                Assert.That(mockLog.LogEvents.Count(e => e.Item1 == LogLevel.Debug && e.Item2.Message.StartsWith($"Received {size} bytes (id {correlationId})")), Is.EqualTo(0));

                server.DropConnection();

                // send half of payload should be skipped
                var lastBytes = new byte[size];
                random.NextBytes(lastBytes);
                while (!await server.SendDataAsync(new ArraySegment<byte>(lastBytes))) {
                    // repeat until the connection is all up and working ...
                }
                await TaskTest.WaitFor(() => bytesRead >= size);
                var received = await TaskTest.WaitFor(() => mockLog.LogEvents.Count(e => e.Item1 == LogLevel.Debug && e.Item2.Message.StartsWith($"Received {size} bytes (id {correlationId})")) == 1);
                Assert.True(received);
            }
        }

        [Test]
        public async Task ReadShouldIgnoreMessageWithUnknownCorrelationId()
        {
            const int correlationId = 99;
            var receivedData = false;

            var mockLog = new MemoryLog();

            var config = new ConnectionConfiguration(onRead: (e, buffer, elapsed) => receivedData = true);
            var endpoint = await Endpoint.ResolveAsync(TestConfig.ServerUri(), TestConfig.Log);
            using (var server = new TcpServer(endpoint.Ip.Port, TestConfig.Log))
            using (var conn = new Connection(endpoint, config, log: mockLog))
            {
                //send correlation message
                await server.SendDataAsync(new ArraySegment<byte>(CreateCorrelationMessage(correlationId)));

                //wait for connection
                await Task.WhenAny(server.ClientConnected, Task.Delay(TimeSpan.FromSeconds(3)));
                await TaskTest.WaitFor(() => receivedData);

                // shortly after receivedData, but still after
                var found = await TaskTest.WaitFor(() => mockLog.LogEvents.Any(e => e.Item1 == LogLevel.Warn && e.Item2.Message == $"Unexpected response (id {correlationId}, 4? bytes) from {endpoint}"));
                Assert.True(found);
            }
        }

        [Test]
        public async Task ReadShouldCancelWhileAwaitingResponse()
        {
            var count = 0;
            var semaphore = new SemaphoreSlim(0);
            var config = new ConnectionConfiguration(onReadingBytes: (e, available) =>
            {
                Interlocked.Increment(ref count);
                semaphore.Release();
                
            });
            var endpoint = await Endpoint.ResolveAsync(TestConfig.ServerUri(), TestConfig.Log);
            using (var server = new TcpServer(endpoint.Ip.Port, TestConfig.Log))
            using (var conn = new Connection(endpoint, config, log: TestConfig.Log))
            {
                var token = new CancellationTokenSource();

                var taskResult = conn.SendAsync(new FetchRequest(), token.Token);

                Thread.Sleep(100);
                token.Cancel();

                semaphore.Wait(TimeSpan.FromSeconds(1));
                Assert.That(count, Is.GreaterThanOrEqualTo(1), "Read should have cancelled and incremented count.");
                Assert.That(taskResult.IsCanceled, Is.True);
            }
        }

        [Test]
        public async Task ReadShouldCancelWhileAwaitingReconnection()
        {
            int connectionAttempt = 0;
            var config = new ConnectionConfiguration(onConnecting: (e, attempt, elapsed) => connectionAttempt = attempt);
            var endpoint = await Endpoint.ResolveAsync(TestConfig.ServerUri(), TestConfig.Log);
            using (var conn = new Connection(endpoint, config, TestConfig.Log))
            using (var token = new CancellationTokenSource())
            {
                var taskResult = conn.SendAsync(new FetchRequest(), token.Token);

                await TaskTest.WaitFor(() => connectionAttempt > 1);

                token.Cancel();

                await Task.WhenAny(taskResult, Task.Delay(500));

                Assert.That(taskResult.IsCanceled, Is.True);
            }
        }

        [Test]
        public async Task ReadShouldReconnectEvenAfterCancelledRead()
        {
            int connectionAttempt = 0;
            var config = new ConnectionConfiguration(onConnecting: (e, attempt, elapsed) => Interlocked.Exchange(ref connectionAttempt, attempt));
            var endpoint = await Endpoint.ResolveAsync(TestConfig.ServerUri(), TestConfig.Log);
            using (var conn = new Connection(endpoint, config, TestConfig.Log))
            using (var token = new CancellationTokenSource())
            {
                var taskResult = conn.SendAsync(new FetchRequest(), token.Token);

                await TaskTest.WaitFor(() => connectionAttempt > 1);

                var attemptsMadeSoFar = connectionAttempt;

                token.Cancel();

                await TaskTest.WaitFor(() => connectionAttempt > attemptsMadeSoFar);

                Assert.That(connectionAttempt, Is.GreaterThan(attemptsMadeSoFar));
            }
        }

        [Test]
        public async Task ShouldReconnectAfterLosingConnectionAndBeAbleToStartNewRead()
        {
            var log = TestConfig.Log;
            var endpoint = await Endpoint.ResolveAsync(TestConfig.ServerUri(), TestConfig.Log);
            using (var server = new TcpServer(endpoint.Ip.Port, TestConfig.Log)) {
                var serverDisconnects = 0;
                var serverConnects = 0;
                var clientDisconnects = 0;
                var clientReads = 0;
                var clientBytesRead = 0;

                server.OnConnected = () => Interlocked.Increment(ref serverConnects);
                server.OnDisconnected = () => Interlocked.Increment(ref serverDisconnects);
                var config = new ConnectionConfiguration(
                    onDisconnected: (e, exception) => Interlocked.Increment(ref clientDisconnects),
                    onReading:(e, available) => Interlocked.Increment(ref clientReads),
                    onRead: (e, read, elapsed) => Interlocked.Add(ref clientBytesRead, read));
                using (var conn = new Connection(endpoint, config, log)) {
                    await TaskTest.WaitFor(() => serverConnects > 0);
                    await TaskTest.WaitFor(() => clientReads > 0, 1000);

                    server.DropConnection();

                    await TaskTest.WaitFor(() => clientDisconnects > 0, 10000);
                    Assert.That(clientDisconnects, Is.AtLeast(1), "The client should have disconnected.");
                    Assert.That(clientBytesRead, Is.EqualTo(0), "client should not have received any bytes.");

                    await TaskTest.WaitFor(() => serverConnects == 2, 6000);
                    Assert.That(serverConnects, Is.EqualTo(2), "Socket should have reconnected.");

                    await server.SendDataAsync(new ArraySegment<byte>(8.ToBytes()));
                    await server.SendDataAsync(new ArraySegment<byte>(99.ToBytes()));
                    await TaskTest.WaitFor(() => clientBytesRead == 8, 1000);
                    Assert.That(clientBytesRead, Is.AtLeast(4), "client should have read the 8 bytes.");
                }
            }
        }

        #endregion

        #region Send

        [Test]
        public async Task SendAsyncShouldTimeoutWhenSendAsyncTakesTooLong()
        {
            var endpoint = await Endpoint.ResolveAsync(TestConfig.ServerUri(), TestConfig.Log);
            using (var server = new TcpServer(endpoint.Ip.Port, TestConfig.Log))
            using (var conn = new Connection(endpoint, new ConnectionConfiguration(requestTimeout: TimeSpan.FromMilliseconds(1)), log: TestConfig.Log))
            {
                await Task.WhenAny(server.ClientConnected, Task.Delay(TimeSpan.FromSeconds(3)));

                var sendTask = conn.SendAsync(new MetadataRequest(), CancellationToken.None);
                await Task.WhenAny(sendTask, Task.Delay(100));

                Assert.That(sendTask.IsFaulted, Is.True, "Task should have reported an exception.");
                Assert.That(sendTask.Exception.InnerException, Is.TypeOf<TimeoutException>());
            }
        }

        [Test]
        public async Task SendAsyncShouldNotAllowResponseToTimeoutWhileAwaitingKafkaToEstableConnection()
        {
            var endpoint = await Endpoint.ResolveAsync(TestConfig.ServerUri(), TestConfig.Log);
            using (var conn = new Connection(endpoint, new ConnectionConfiguration(requestTimeout: TimeSpan.FromSeconds(1000)), log: TestConfig.Log))
            {
                // SendAsync blocked by reconnection attempts
                var taskResult = conn.SendAsync(new MetadataRequest(), CancellationToken.None);

                // Task result should be WaitingForActivation
                Assert.That(taskResult.IsFaulted, Is.False);
                Assert.That(taskResult.Status, Is.EqualTo(TaskStatus.WaitingForActivation));

                // Starting server to establish connection
                using (var server = new TcpServer(endpoint.Ip.Port, TestConfig.Log))
                {
                    server.OnConnected = () => TestConfig.Log.Info(() => LogEvent.Create("Client connected..."));
                    server.OnBytesReceived = b => {
                        var requestContext = KafkaDecoder.DecodeHeader(b.Skip(KafkaEncoder.IntegerByteSize));
                        AsyncContext.Run(async () => await server.SendDataAsync(MessageHelper.CreateMetadataResponse(requestContext, "Test")));
                    };

                    await Task.WhenAny(taskResult, Task.Delay(TimeSpan.FromSeconds(5)));

                    Assert.That(taskResult.IsFaulted, Is.False);
                    Assert.That(taskResult.IsCanceled, Is.False);
                    await taskResult;
                    Assert.That(taskResult.Status, Is.EqualTo(TaskStatus.RanToCompletion));
                }
            }
        }

        [Test]
        public async Task SendAsyncShouldUseStatictVersionInfo()
        {
            IRequestContext context = null;
            var endpoint = await Endpoint.ResolveAsync(TestConfig.ServerUri(), TestConfig.Log);
            using (var server = new TcpServer(endpoint.Ip.Port, TestConfig.Log))
            using (var conn = new Connection(endpoint, new ConnectionConfiguration(requestTimeout: TimeSpan.FromSeconds(1000), versionSupport: VersionSupport.Kafka10), log: TestConfig.Log))
            {
                server.OnBytesReceived = data =>
                {
                    context = KafkaDecoder.DecodeHeader(data.Skip(KafkaEncoder.IntegerByteSize));
                    var send = server.SendDataAsync(KafkaDecoder.EncodeResponseBytes(context, new FetchResponse()));
                };

                await conn.SendAsync(new FetchRequest(new FetchRequest.Topic("Foo", 0, 0)), CancellationToken.None);
                await TaskTest.WaitFor(() => context != null);

                Assert.That(context.ApiVersion.Value, Is.EqualTo(2));
            }
        }

        [Test]
        public async Task SendAsyncShouldTimeoutMultipleMessagesAtATime()
        {
            var endpoint = await Endpoint.ResolveAsync(TestConfig.ServerUri(), TestConfig.Log);
            using (var server = new TcpServer(endpoint.Ip.Port, TestConfig.Log))
            using (var conn = new Connection(endpoint, new ConnectionConfiguration(requestTimeout: TimeSpan.FromMilliseconds(100)), log: TestConfig.Log))
            {
                await Task.WhenAny(server.ClientConnected, Task.Delay(TimeSpan.FromSeconds(3)));

                var tasks = new[] {
                    conn.SendAsync(new MetadataRequest(), CancellationToken.None),
                    conn.SendAsync(new MetadataRequest(), CancellationToken.None),
                    conn.SendAsync(new MetadataRequest(), CancellationToken.None)
                };

                await TaskTest.WaitFor(() => tasks.All(t => t.IsFaulted));
                foreach (var task in tasks)
                {
                    Assert.That(task.IsFaulted, Is.True, "Task should have faulted.");
                    Assert.That(task.Exception.InnerException, Is.TypeOf<TimeoutException>(), "Task fault has wrong type.");
                }
            }
        }

        #endregion

        private static byte[] CreateCorrelationMessage(int id)
        {
            var buffer = new byte[8];
            var stream = new MemoryStream(buffer);
            stream.Write(KafkaEncoder.CorrelationSize.ToBytes(), 0, 4);
            stream.Write(id.ToBytes(), 0, 4);
            return buffer;
        }
    }
}