using System;

namespace KafkaClient
{
    public class ConsumerConfiguration : IConsumerConfiguration
    {
        public ConsumerConfiguration(int? maxFetchBytes = null, TimeSpan? maxServerWait = null)
        {
            MaxFetchBytes = maxFetchBytes;
            MaxServerWait = maxServerWait;
        }

        public int? MaxFetchBytes { get; }
        public TimeSpan? MaxServerWait { get; }
    }
}