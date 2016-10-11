using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;
using KafkaClient.Protocol.Types;

namespace KafkaClient.Connections
{
    /// <summary>
    /// Configuration for the tcp connection.
    /// </summary>
    public class ConnectionConfiguration : IConnectionConfiguration
    {
        /// <summary>
        /// Configuration for the tcp connection.
        /// </summary>
        /// <param name="connectionTimeout">The total timeout to use for the connection attempts.</param>
        public ConnectionConfiguration(TimeSpan connectionTimeout)
            : this(Defaults.ConnectionRetry(connectionTimeout))
        {
        }

        /// <summary>
        /// Configuration for the tcp connection.
        /// </summary>
        /// <param name="connectionRetry">Retry details for (re)establishing the connection.</param>
        /// <param name="versionSupport">Support for different protocol versions for Kakfa requests and responses.</param>
        /// <param name="requestTimeout">The maximum time to wait for requests.</param>
        /// <param name="encoders">Custom Encoding support for different protocol types</param>
        /// <param name="onDisconnected">Triggered when the tcp socket is disconnected.</param>
        /// <param name="onConnecting">Triggered when the tcp socket is connecting.</param>
        /// <param name="onConnected">Triggered after the tcp socket is successfully connected.</param>
        /// <param name="onWriteEnqueued">Triggered after enqueing async write task for writing to the tcp stream.</param>
        /// <param name="onWriting">Triggered when writing to the tcp stream.</param>
        /// <param name="onWritten">Triggered after having successfully written to the tcp stream.</param>
        /// <param name="onWriteFailed">Triggered after failing to write to the tcp stream.</param>
        /// <param name="onReading">Triggered when starting to read a message's bytes from the tcp stream.</param>
        /// <param name="onReadingChunk">Triggered when reading a chunk of bytes from the tcp stream.</param>
        /// <param name="onReadChunk">Triggered after successfully reading a chunk of bytes from the tcp stream.</param>
        /// <param name="onRead">Triggered after having successfully read a message's bytes from the tcp stream.</param>
        /// <param name="onReadFailed">Triggered after failing to read from the tcp stream.</param>
        public ConnectionConfiguration(
            IRetry connectionRetry = null, 
            IVersionSupport versionSupport = null,
            TimeSpan? requestTimeout = null,
            IEnumerable<IProtocolTypeEncoder> encoders = null,
            ConnectError onDisconnected = null, 
            Connecting onConnecting = null, 
            Connecting onConnected = null, 
            Writing onWriteEnqueued = null, 
            Writing onWriting = null, 
            WriteSuccess onWritten = null, 
            WriteError onWriteFailed = null, 
            Reading onReading = null, 
            ReadingChunk onReadingChunk = null, 
            ReadChunk onReadChunk = null, 
            Read onRead = null, 
            ReadError onReadFailed = null
            )
        {
            ConnectionRetry = connectionRetry ?? Defaults.ConnectionRetry();
            VersionSupport = versionSupport ?? Connections.VersionSupport.Kafka8;
            RequestTimeout = requestTimeout ?? TimeSpan.FromSeconds(Defaults.RequestTimeoutSeconds);
            Encoders = encoders != null
                ? encoders.ToImmutableDictionary(e => e.Type)
                : ImmutableDictionary<string, IProtocolTypeEncoder>.Empty;
            OnDisconnected = onDisconnected;
            OnConnecting = onConnecting;
            OnConnected = onConnected;
            OnWriteEnqueued = onWriteEnqueued;
            OnWriting = onWriting;
            OnWritten = onWritten;
            OnWriteFailed = onWriteFailed;
            OnReading = onReading;
            OnReadingChunk = onReadingChunk;
            OnReadChunk = onReadChunk;
            OnRead = onRead;
            OnReadFailed = onReadFailed;
        }

        /// <inheritdoc />
        public IRetry ConnectionRetry { get; }

        /// <inheritdoc />
        public IVersionSupport VersionSupport { get; }

        /// <inheritdoc />
        public TimeSpan RequestTimeout { get; }

        /// <inheritdoc />
        public IImmutableDictionary<string, IProtocolTypeEncoder> Encoders { get; }

        /// <inheritdoc />
        public ConnectError OnDisconnected { get; }

        /// <inheritdoc />
        public Connecting OnConnecting { get; }

        /// <inheritdoc />
        public Connecting OnConnected { get; }

        /// <inheritdoc />
        public Writing OnWriteEnqueued { get; }

        /// <inheritdoc />
        public Writing OnWriting { get; }

        /// <inheritdoc />
        public WriteSuccess OnWritten { get; }

        /// <inheritdoc />
        public WriteError OnWriteFailed { get; }

        /// <inheritdoc />
        public Reading OnReading { get; }

        /// <inheritdoc />
        public ReadingChunk OnReadingChunk { get; }

        /// <inheritdoc />
        public ReadChunk OnReadChunk { get; }

        /// <inheritdoc />
        public Read OnRead { get; }

        /// <inheritdoc />
        public ReadError OnReadFailed { get; }

        public static class Defaults
        {
            /// <summary>
            /// The default <see cref="ConnectionConfiguration.RequestTimeout"/> seconds
            /// </summary>
            public const int RequestTimeoutSeconds = 60;

            /// <summary>
            /// The default <see cref="ConnectionConfiguration.ConnectionRetry"/> timeout
            /// </summary>
            public const int ConnectingTimeoutMinutes = 5;

            /// <summary>
            /// The default max retries for <see cref="ConnectionConfiguration.ConnectionRetry"/>
            /// </summary>
            public const int MaxConnectionAttempts = 6;

            /// <summary>
            /// The default <see cref="ConnectionConfiguration.ConnectionRetry"/> backoff delay
            /// </summary>
            public const int ConnectingDelayMilliseconds = 100;

            public static IRetry ConnectionRetry(TimeSpan? timeout = null)
            {
                return new BackoffRetry(
                    timeout ?? TimeSpan.FromMinutes(ConnectingTimeoutMinutes),
                    TimeSpan.FromMilliseconds(ConnectingDelayMilliseconds), 
                    MaxConnectionAttempts);
            }
        }
    }
}