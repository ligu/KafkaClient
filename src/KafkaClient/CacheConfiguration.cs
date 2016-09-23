using System;
using KafkaClient.Common;

namespace KafkaClient
{
    public class CacheConfiguration : ICacheConfiguration
    {
        public CacheConfiguration(IRetry refreshRetry = null, TimeSpan? cacheExpiration = null)
        {
            RefreshRetry = refreshRetry ?? DefaultRefreshRetry();
            CacheExpiration = cacheExpiration ?? TimeSpan.FromMilliseconds(DefaultCacheExpirationMilliseconds);
        }

        public IRetry RefreshRetry { get; }
        public TimeSpan CacheExpiration { get; }

        public const int DefaultRefreshTimeoutSeconds = 200;
        public const int DefaultMaxRetries = 3;
        public const int DefaultCacheExpirationMilliseconds = 10;


        public static IRetry DefaultRefreshRetry(TimeSpan? timeout = null)
        {
            return new NoRetry(timeout ?? TimeSpan.FromSeconds(DefaultRefreshTimeoutSeconds));
        }
    }
}