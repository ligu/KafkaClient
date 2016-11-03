using System;
using System.Runtime.CompilerServices;
using KafkaClient.Common;

namespace KafkaClient.Tests.Helpers
{
    public static class IntegrationConfig
    {
        public const int TestAttempts = 1;

        public static string TopicName([CallerMemberName] string name = null)
        {
            return $"{Environment.MachineName}-Topic-{name}";
        }

        public static string ConsumerName([CallerMemberName] string name = null)
        {
            return $"{Environment.MachineName}-Consumer-{name}";
        }

        // Some of the tests measured performance.my log is too slow so i change the log level to
        // only critical message
        public static ILog NoDebugLog = new ConsoleLog(LogLevel.Info);

        public static ILog AllLog = new ConsoleLog();

        public static Uri IntegrationUri { get; } = new Uri("http://kafka1:9092");
    }
}