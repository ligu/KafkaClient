using System;

namespace KafkaClient
{
    public interface IConsumerConfiguration
    {
        int? MaxFetchBytes { get; }
        TimeSpan? MaxServerWait { get; }
    }
}