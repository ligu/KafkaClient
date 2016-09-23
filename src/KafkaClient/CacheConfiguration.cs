using System;

namespace KafkaClient
{
    public class CacheConfiguration : ICacheConfiguration
    {
        public CacheConfiguration(TimeSpan? refreshTimeout = null, int? maxRetries = null, TimeSpan? cacheExpiration = null)
        {
            RefreshTimeout = refreshTimeout ?? TimeSpan.FromSeconds(DefaultRefreshTimeoutSeconds);
            MaxRetries = maxRetries.GetValueOrDefault(DefaultMaxRetries);
            CacheExpiration = cacheExpiration ?? TimeSpan.FromMilliseconds(DefaultCacheExpirationMilliseconds);
        }

        public TimeSpan RefreshTimeout { get; }
        public int MaxRetries { get; }
        public TimeSpan CacheExpiration { get; }

        public const int DefaultRefreshTimeoutSeconds = 200;
        public const int DefaultMaxRetries = 3;
        public const int DefaultCacheExpirationMilliseconds = 10;
    }
}