using System;
using KafkaClient.Common;

namespace KafkaClient.Connection
{
    public interface IConnectionConfiguration
    {
        bool TrackTelemetry { get; }

        /// <summary>
        /// Retry details for the connection itself.
        /// </summary>
        IRetry ConnectionRetry { get; }

        /// <summary>
        /// The maximum time to wait for requests.
        /// </summary>
        TimeSpan RequestTimeout { get; }
    }
}