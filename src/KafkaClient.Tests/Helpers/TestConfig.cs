using System;
using System.Runtime.CompilerServices;
using KafkaClient.Common;

namespace KafkaClient.Tests.Helpers
{
    public static class TestConfig
    {
        public static string TopicName([CallerMemberName] string name = null)
        {
            return $"{Environment.MachineName}-Topic-{name}";
        }

        public static string ConsumerName([CallerMemberName] string name = null)
        {
            return $"{Environment.MachineName}-Consumer-{name}";
        }

        // turned down to reduce log noise -- turn up if necessary
        public static ILog Log = new ConsoleLog(LogLevel.Warn);

        public static Uri ServerUri([CallerMemberName] string name = null)
        {
            return new Uri($"http://localhost:{ServerPort(name)}");
        }

        public static int ServerPort([CallerMemberName] string name = null)
        {
            return 10000 + (name ?? "").GetHashCode() % 1000;
        }

        //public static Uri IntegrationUri { get; } = new Uri("http://kafka1:9092");
        public static Uri IntegrationUri { get; } = new Uri("http://kafkaclient.westus.cloudapp.azure.com:9092");
    }
}