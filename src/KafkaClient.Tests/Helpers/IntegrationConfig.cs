using System;
using System.Configuration;
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
        public static ILog NoDebugLog = new TraceLog(LogLevel.Info);

        public static ILog AllLog = new TraceLog();

        public static Uri IntegrationUri
        {
            get
            {
                var url = ConfigurationManager.AppSettings["IntegrationKafkaServerUrl"];
                if (url == null) throw new ConfigurationErrorsException("IntegrationKafkaServerUrl must be specified in the app.config file.");
                return new Uri(url);
            }
        }
    }
}