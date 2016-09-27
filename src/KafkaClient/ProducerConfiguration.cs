using System;
using KafkaClient.Protocol;

namespace KafkaClient
{
    public class ProducerConfiguration : IProducerConfiguration
    {
        public ProducerConfiguration(
            int? requestParallelization = null, 
            int? batchSize = null, 
            TimeSpan? batchMaxDelay = null,
            MessageCodec codec = MessageCodec.CodecNone,
            short? acks = null,
            TimeSpan? serverAckTimeout = null,
            TimeSpan? stopTimeout = null)
        {
            RequestParallelization = requestParallelization ?? Defaults.RequestParallelization;
            BatchSize = batchSize ?? Defaults.BatchSize;
            BatchMaxDelay = batchMaxDelay ?? TimeSpan.FromMilliseconds(Defaults.BatchMaxDelayMilliseconds);
            Codec = codec;
            Acks = acks ?? 1;
            ServerAckTimeout = serverAckTimeout ?? TimeSpan.FromSeconds(Defaults.ServerAckTimeoutSeconds);
            StopTimeout = stopTimeout ?? TimeSpan.FromSeconds(Defaults.DefaultStopTimeoutSeconds);
        }

        /// <inheritdoc />
        public int RequestParallelization { get; }

        /// <inheritdoc />
        public int BatchSize { get; }

        /// <inheritdoc />
        public TimeSpan BatchMaxDelay { get; }

        /// <inheritdoc />
        public MessageCodec Codec { get; }

        /// <inheritdoc />
        public short Acks { get; }

        /// <inheritdoc />
        public TimeSpan ServerAckTimeout { get; }

        /// <inheritdoc />
        public TimeSpan StopTimeout { get; }

        public static class Defaults
        {
            /// <summary>
            /// The default value for <see cref="ProducerConfiguration.RequestParallelization"/>
            /// </summary>
            public const int RequestParallelization = 20;

            /// <summary>
            /// The default value for <see cref="BatchMaxDelay"/>
            /// </summary>
            public const int BatchMaxDelayMilliseconds = 100;

            /// <summary>
            /// The default value for <see cref="ProducerConfiguration.BatchSize"/>
            /// </summary>
            public const int BatchSize = 100;

            /// <summary>
            /// The default value for <see cref="ProducerConfiguration.ServerAckTimeout"/>
            /// </summary>
            public const int ServerAckTimeoutSeconds = 1;

            /// <summary>
            /// The default value for <see cref="StopTimeout"/>
            /// </summary>
            public const int DefaultStopTimeoutSeconds = 30;
        }
    }
}