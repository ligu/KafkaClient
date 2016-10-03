using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Connection;
using KafkaClient.Protocol;
using KafkaClient.Tests.Fakes;
using KafkaClient.Tests.Helpers;
using Moq;
using Ninject.MockingKernel.Moq;
using NUnit.Framework;

namespace KafkaClient.Tests.Unit
{
    [Category("Integration")]
    [TestFixture]
    public class KafkaConnectionTests
    {
        private readonly TraceLog _log;
        private readonly Endpoint _endpoint;
        private MoqMockingKernel _kernel;

        public KafkaConnectionTests()
        {
            _log = new TraceLog();
            _endpoint = new ConnectionFactory().Resolve(new Uri("http://localhost:8999"), _log);
        }

        [SetUp]
        public void Setup()
        {
            _kernel = new MoqMockingKernel();
        }

        #region Construct...

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task ShouldStartReadPollingOnConstruction()
        {
            using (var socket = new TcpSocket(_endpoint, log: _log))
            using (var conn = new Connection.Connection(socket, log: _log))
            {
                await TaskTest.WaitFor(() => conn.IsReaderAlive);
                Assert.That(conn.IsReaderAlive, Is.True);
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void ShouldReportServerUriOnConstruction()
        {
            var expectedUrl = _endpoint;
            using (var socket = new TcpSocket(expectedUrl, log: _log))
            using (var conn = new Connection.Connection(socket, log: _log))
            {
                Assert.That(conn.Endpoint, Is.EqualTo(expectedUrl));
            }
        }

        #endregion Construct...

        #region Dispose Tests...

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task ShouldDisposeWithoutExceptionThrown()
        {
            using (var server = new FakeTcpServer(_log, 8999))
            using (var socket = new TcpSocket(_endpoint, log: _log))
            {
                var conn = new Connection.Connection(socket, log: _log);
                await TaskTest.WaitFor(() => server.ConnectionEventcount > 0);
                using (conn) { }
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void ShouldDisposeWithoutExceptionEvenWhileCallingSendAsync()
        {
            using (var socket = new TcpSocket(_endpoint, log: _log))
            using (var conn = new Connection.Connection(socket, log: _log))
            {
                var task = conn.SendAsync(new MetadataRequest(), CancellationToken.None);
                task.Wait(TimeSpan.FromMilliseconds(1000));
                Assert.That(task.IsCompleted, Is.False, "The send task should still be pending.");
            }
        }

        #endregion Dispose Tests...

        #region Read Tests...

    
        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task KafkaConnectionShouldLogDisconnectAndRecover()
        {
            var mockLog = new MemoryLog();
            var log = new TraceLog(LogLevel.Error);
            var disconnected = 0;

            var config = new ConnectionConfiguration(onDisconnected: (endpoint, exception) => {
                                                         Interlocked.Increment(ref disconnected);
                                                     });
            using (var server = new FakeTcpServer(log, 8999))
            using (var socket = new TcpSocket(_endpoint, config, log: log))
            using (var conn = new Connection.Connection(socket, config, log: mockLog))
            {
                for (int connectionAttempt = 1; connectionAttempt < 4; connectionAttempt++)
                {

                    await TaskTest.WaitFor(() => server.ConnectionEventcount == connectionAttempt);
                    Assert.That(server.ConnectionEventcount, Is.EqualTo(connectionAttempt));
                    server.SendDataAsync(CreateCorrelationMessage(1)).Wait(TimeSpan.FromSeconds(5));
                    await TaskTest.WaitFor(() => !conn.IsInErrorState);

                    Assert.IsFalse(conn.IsInErrorState);
                    Assert.That(mockLog.LogEvents.Count(e => e.Item1 == LogLevel.Info && e.Item2.Message.StartsWith("Polling read thread has recovered: ")), Is.EqualTo(connectionAttempt-1));

                    server.DropConnection();
                    await TaskTest.WaitFor(() => conn.IsInErrorState);
                    Assert.AreEqual(disconnected, connectionAttempt);
                    Assert.IsTrue(conn.IsInErrorState);

                    Assert.That(mockLog.LogEvents.Count(e => e.Item1 == LogLevel.Error && e.Item2.Message.StartsWith("Exception occured in polling read thread")), Is.EqualTo(connectionAttempt));
                }

            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task ReadShouldIgnoreMessageWithUnknownCorrelationId()
        {
            const int correlationId = 99;
            var receivedData = false;

            var mockLog = new MemoryLog();

            var config = new ConnectionConfiguration(onRead: (endpoint, buffer, elapsed) => receivedData = true);
            using (var server = new FakeTcpServer(_log, 8999))
            using (var socket = new TcpSocket(_endpoint, config, log: mockLog))
            using (var conn = new Connection.Connection(socket, config, log: mockLog))
            {
                //send correlation message
                server.SendDataAsync(CreateCorrelationMessage(correlationId)).Wait(TimeSpan.FromSeconds(5));

                //wait for connection
                await TaskTest.WaitFor(() => server.ConnectionEventcount > 0);
                Assert.That(server.ConnectionEventcount, Is.EqualTo(1));

                await TaskTest.WaitFor(() => receivedData);

                //should log a warning and keep going
                Assert.That(mockLog.LogEvents.Any(e => e.Item1 == LogLevel.Warn && e.Item2.Message == $"Unexpected Response from {_endpoint} with CorrelationId {correlationId} (not in request queue)."));
            }
        }

        #endregion Read Tests...

        #region Send Tests...

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task SendAsyncShouldTimeoutWhenSendAsyncTakesTooLong()
        {
            using (var server = new FakeTcpServer(_log, 8999))
            using (var socket = new TcpSocket(_endpoint, log: _log))
            using (var conn = new Connection.Connection(socket, new ConnectionConfiguration(requestTimeout: TimeSpan.FromMilliseconds(1)), log: _log))
            {
                await TaskTest.WaitFor(() => server.ConnectionEventcount > 0);
                Assert.That(server.ConnectionEventcount, Is.EqualTo(1));

                var taskResult = conn.SendAsync(new MetadataRequest(), CancellationToken.None);

                taskResult.ContinueWith(t => taskResult = t).Wait(TimeSpan.FromMilliseconds(100));

                Assert.That(taskResult.IsFaulted, Is.True, "Task should have reported an exception.");
                Assert.That(taskResult.Exception.InnerException, Is.TypeOf<TimeoutException>());
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task SendAsyncShouldNotAllowResponseToTimeoutWhileAwaitingKafkaToEstableConnection()
        {
            using (var socket = new TcpSocket(_endpoint, log: _log))
            using (var conn = new Connection.Connection(socket, new ConnectionConfiguration(requestTimeout: TimeSpan.FromSeconds(1000)), log: _log))
            {
                Console.WriteLine("SendAsync blocked by reconnection attempts...");
                var taskResult = conn.SendAsync(new MetadataRequest(), CancellationToken.None);

                Console.WriteLine("Task result should be WaitingForActivation...");
                Assert.That(taskResult.IsFaulted, Is.False);
                Assert.That(taskResult.Status, Is.EqualTo(TaskStatus.WaitingForActivation));

                Console.WriteLine("Starting server to establish connection...");
                using (var server = new FakeTcpServer(_log, 8999))
                {
                    server.OnClientConnected += () => Console.WriteLine("Client connected...");
                    server.OnBytesReceived += (b) =>
                    {
                        var send = server.SendDataAsync(MessageHelper.CreateMetadataResponse(1, "Test"));
                    };

                    await Task.WhenAny(taskResult, Task.Delay(TimeSpan.FromSeconds(15)));

                    Assert.That(taskResult.IsFaulted, Is.False);
                    Assert.That(taskResult.IsCanceled, Is.False);
                    Assert.That(taskResult.Status, Is.EqualTo(TaskStatus.RanToCompletion));
                }
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task SendAsyncShouldTimeoutMultipleMessagesAtATime()
        {
            using (var server = new FakeTcpServer(_log, 8999))
            using (var socket = new TcpSocket(_endpoint, log: _log))
            using (var conn = new Connection.Connection(socket, new ConnectionConfiguration(requestTimeout: TimeSpan.FromMilliseconds(100)), log: _log))
            {
                server.HasClientConnected.Wait(TimeSpan.FromSeconds(3));
                Assert.That(server.ConnectionEventcount, Is.EqualTo(1));

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

        #endregion Send Tests...

        private static byte[] CreateCorrelationMessage(int id)
        {
            return new MessagePacker().Pack(id).Payload();
        }
    }
}