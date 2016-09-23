using System;
using KafkaClient.Common;

namespace KafkaClient.Connection
{
    public class ConnectionConfiguration : IConnectionConfiguration
    {
        public ConnectionConfiguration(IRetry connectionRetry = null, TimeSpan? requestTimeout = null, bool trackTelemetry = false)
        {
            ConnectionRetry = connectionRetry ??
                              new BackoffRetry(
                                  TimeSpan.FromMinutes(DefaultConnectingTimeoutMinutes),
                                  TimeSpan.FromMilliseconds(DefaultConnectingDelayMilliseconds),
                                  DefaultMaxRetries);
            RequestTimeout = requestTimeout ?? TimeSpan.FromSeconds(DefaultRequestTimeoutSeconds);
            TrackTelemetry = trackTelemetry;
        }

        public bool TrackTelemetry { get; }
        public IRetry ConnectionRetry { get; }
        public TimeSpan RequestTimeout { get; }

        /// <summary>
        /// The default RequestRetry timeout
        /// </summary>
        public const int DefaultRequestTimeoutSeconds = 60;

        /// <summary>
        /// The default ConnectionRetry timeout
        /// </summary>
        public const int DefaultConnectingTimeoutMinutes = 5;

        /// <summary>
        /// The default max retries for ConnectionRetry and RequestRetry
        /// </summary>
        public const int DefaultMaxRetries = 5;

        /// <summary>
        /// The default ConnectionRetry backoff delay
        /// </summary>
        public const int DefaultConnectingDelayMilliseconds = 100;

        public static IRetry DefaultConnectionRetry(TimeSpan? timeout = null)
        {
            return new BackoffRetry(
                timeout ?? TimeSpan.FromMinutes(DefaultConnectingTimeoutMinutes),
                TimeSpan.FromMilliseconds(DefaultConnectingDelayMilliseconds),
                DefaultMaxRetries);
        }
    }
}