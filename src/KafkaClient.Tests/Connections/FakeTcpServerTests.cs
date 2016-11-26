using System;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Tests.Fakes;
using KafkaClient.Tests.Helpers;
using NUnit.Framework;

namespace KafkaClient.Tests.Connections
{
    [TestFixture]
    [Category("Unit")]
    public class FakeTcpServerTests
    {
        private readonly ILog _ilog = new ConsoleLog(LogLevel.Warn);

        [Test]
        public async Task FakeShouldBeAbleToReconnect()
        {
            var serverUri = UnitConfig.ServerUri();
            using (var server = new FakeTcpServer(_ilog, serverUri.Port))
            {
                byte[] received = null;
                server.OnBytesReceived += data => received = data;

                var t1 = new TcpClient();
                await t1.ConnectAsync(serverUri.Host, serverUri.Port);
                await TaskTest.WaitFor(() => server.ConnectionEventcount == 1);

                server.DropConnection();
                await TaskTest.WaitFor(() => server.DisconnectionEventCount == 1);

                var t2 = new TcpClient();
                await t2.ConnectAsync(serverUri.Host, serverUri.Port);
                await TaskTest.WaitFor(() => server.ConnectionEventcount == 2);

                t2.GetStream().Write(99.ToBytes(), 0, 4);
                await TaskTest.WaitFor(() => received != null);

                Assert.That(received.ToInt32(), Is.EqualTo(99));
            }
        }

        [Test]
        public void ShouldDisposeEvenWhenTryingToSendWithoutExceptionThrown()
        {
            using (var server = new FakeTcpServer(_ilog, UnitConfig.ServerPort()))
            {
                server.SendDataAsync("test");
                Thread.Sleep(500);
            }
        }

        [Test]
        public void ShouldDisposeWithoutExecptionThrown()
        {
            using (var server = new FakeTcpServer(_ilog, UnitConfig.ServerPort()))
            {
                Thread.Sleep(500);
            }
        }

        [Test]
        public async Task SendAsyncShouldWaitUntilClientIsConnected()
        {
            const int testData = 99;
            var serverUri = UnitConfig.ServerUri();
            using (var server = new FakeTcpServer(_ilog, serverUri.Port))
            using (var client = new TcpClient())
            {
                var send = server.SendDataAsync(testData.ToBytes());
                Thread.Sleep(1000);
                await client.ConnectAsync(serverUri.Host, serverUri.Port);

                var buffer = new byte[4];
                client.GetStream().ReadAsync(buffer, 0, 4).Wait(TimeSpan.FromSeconds(5));

                Assert.That(buffer.ToInt32(), Is.EqualTo(testData));
            }
        }
    }
}