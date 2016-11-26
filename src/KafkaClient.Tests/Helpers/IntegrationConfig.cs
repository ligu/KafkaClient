using System;
using System.Runtime.CompilerServices;
using KafkaClient.Common;

namespace KafkaClient.Tests.Helpers
{
    public static class IntegrationConfig
    {
        public static string TopicName([CallerMemberName] string name = null)
        {
            return $"{Environment.MachineName}-Topic-{name}";
        }

        public static string ConsumerName([CallerMemberName] string name = null)
        {
            return $"{Environment.MachineName}-Consumer-{name}";
        }

        public static ILog WarnLog = new ConsoleLog(LogLevel.Warn);

        public static ILog InfoLog = new ConsoleLog(LogLevel.Info);

        public static ILog DebugLog = new ConsoleLog();

        public static Uri IntegrationUri { get; } = new Uri("http://kafkaclient.westus.cloudapp.azure.com:9092");
    }
}