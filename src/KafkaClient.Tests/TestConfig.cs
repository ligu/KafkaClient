using System;
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

        // turned down to reduce log noise -- turn up if necessary
        public static ILog Log = new ConsoleLog();

        public static Uri ServerUri([CallerMemberName] string name = null)
        {
            return new Uri($"http://localhost:{ServerPort(name)}");
        }

        public static int ServerPort([CallerMemberName] string name = null)
        {
            return 10000 + (name ?? "").GetHashCode() % 100;
        }

        //public static Uri IntegrationUri { get; } = new Uri("http://kafka1:9092");
        public static Uri IntegrationUri { get; } = new Uri("http://kafkaclient.westus.cloudapp.azure.com:9092");

        public static KafkaOptions Options { get; } = new KafkaOptions(
            IntegrationUri,
            new ConnectionConfiguration(new Retry(TimeSpan.FromSeconds(3), 3)),
            new RouterConfiguration(new Retry(null, 2)),
            log: Log);
    }
}