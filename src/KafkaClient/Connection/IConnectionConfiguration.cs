using System;

namespace KafkaClient.Connection
{
    public interface IConnectionConfiguration
    {
        bool TrackTelemetry { get; }

        /// <summary>
        /// The maximum time to wait when backing off on reconnection attempts.
        /// </summary>
        TimeSpan ConnectingTimeout { get; }

        /// <summary>
        /// The maximum number of retries for (re)establishing a connection.
        /// </summary>
        int MaxRetries { get; }

        /// <summary>
        /// The maximum time to wait for a response from kafka.
        /// </summary>
        TimeSpan RequestTimeout { get; }
    }
}