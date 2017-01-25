using System;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using KafkaClient.Common;
using KafkaClient.Connections;

namespace KafkaClient.Tests
{
    public static class TestConfig
    {
        public static string TopicName([CallerMemberName] string name = null)
        {
            return $"{Environment.MachineName}-Topic-{name}";
        }

        public static string GroupId([CallerMemberName] string name = null)
        {
            return $"{Environment.MachineName}-Group-{name}-{Guid.NewGuid():N}";
        }

        // turned down to reduce log noise -- turn up Level if necessary
        public static ILog Log = new ConsoleLog();

        public static Uri ServerUri()
        {
            return new Uri($"http://localhost:{ServerPort()}");
        }

        public static int ServerPort()
        {
            using (var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)) {
                socket.Bind(new IPEndPoint(IPAddress.Loopback, 0));
                return ((IPEndPoint) socket.LocalEndPoint).Port;
            }
        }

        public static Uri IntegrationUri { get; } = new Uri("http://kafka1:9092");

        public static KafkaOptions Options { get; } = new KafkaOptions(
            IntegrationUri,
            new ConnectionConfiguration(new Retry(TimeSpan.FromSeconds(3), 3)),
            new RouterConfiguration(new Retry(null, 2)),
            consumerConfiguration: new ConsumerConfiguration(heartbeatTimeout: TimeSpan.FromSeconds(10)),
            log: Log);
    }
}