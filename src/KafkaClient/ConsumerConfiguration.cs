using System;

namespace KafkaClient
{
    public class ConsumerConfiguration : IConsumerConfiguration
    {
        public ConsumerConfiguration(
            TimeSpan? maxServerWait = null, 
            int? minFetchBytes = null, 
            int? maxFetchBytes = null, 
            int? maxPartitionFetchBytes = null)
        {
            MaxFetchServerWait = maxServerWait;
            MinFetchBytes = minFetchBytes;
            MaxFetchBytes = maxFetchBytes;
            MaxPartitionFetchBytes = maxPartitionFetchBytes;
        }

        /// <inheritdoc/>
        public int? MaxFetchBytes { get; }
        /// <inheritdoc/>
        public int? MaxPartitionFetchBytes { get; }
        /// <inheritdoc/>
        public int? MinFetchBytes { get; }
        /// <inheritdoc/>
        public TimeSpan? MaxFetchServerWait { get; }
    }
}