using System;

namespace KafkaClient.Connection
{
    public class KafkaConnectionConfiguration : IKafkaConnectionConfiguration
    {
        public KafkaConnectionConfiguration(TimeSpan? connectingTimeout = null, int? maxRetries = null, TimeSpan? requestTimeout = null, bool trackTelemetry = false)
        {
            ConnectingTimeout = connectingTimeout;
            MaxRetries = maxRetries ?? DefaultMaxRetries;
            RequestTimeout = requestTimeout ?? TimeSpan.FromSeconds(DefaultRequestTimeoutSeconds);
            TrackTelemetry = trackTelemetry;
        }

        public bool TrackTelemetry { get; }

        /// <summary>
        /// The maximum time to wait when backing off on reconnection attempts.
        /// </summary>
        public TimeSpan? ConnectingTimeout { get; }

        /// <summary>
        /// The maximum number of retries for (re)establishing a connection.
        /// </summary>
        public int MaxRetries { get; }

        /// <summary>
        /// The maximum time to wait for a response from kafka.
        /// </summary>
        public TimeSpan? RequestTimeout { get; }

        public const int DefaultRequestTimeoutSeconds = 60;
        public const int DefaultMaxRetries = 5;
    }
}