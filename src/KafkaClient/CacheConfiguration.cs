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

        /// <inheritdoc />
        public IRetry RefreshRetry { get; }

        /// <inheritdoc />
        public TimeSpan CacheExpiration { get; }

        /// <summary>
        /// The default timeout for requests made to refresh the cache
        /// </summary>
        public const int DefaultRefreshTimeoutSeconds = 200;

        /// <summary>
        /// The default maximum number of attempts made when refreshing the cache
        /// </summary>
        public const int DefaultMaxRefreshAttempts = 2;

        /// <summary>
        /// The default RefreshRetry backoff delay
        /// </summary>
        public const int DefaultRefreshDelayMilliseconds = 100;

        /// <summary>
        /// The default expiration length for cached topic/partition information
        /// </summary>
        public const int DefaultCacheExpirationMilliseconds = 10;

        public static IRetry DefaultRefreshRetry(TimeSpan? timeout = null)
        {
            return new BackoffRetry(timeout ?? TimeSpan.FromSeconds(DefaultRefreshTimeoutSeconds), 
                TimeSpan.FromMilliseconds(DefaultRefreshDelayMilliseconds),
                DefaultMaxRefreshAttempts);
        }
    }
}