using System;

namespace KafkaClient.Connection
{
    public interface IKafkaConnectionOptions
    {
        bool TrackTelemetry { get; }

        // TODO: better policy around connection retries?

        /// <summary>
        /// The maximum time to wait when backing off on reconnection attempts.
        /// </summary>
        TimeSpan? ConnectingTimeout { get; }

        int MaxRetry { get; }

        /// <summary>
        /// The maximum time to wait for a response from kafka.
        /// </summary>
        TimeSpan? RequestTimeout { get; }
    }
}