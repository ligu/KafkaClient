using System;
using KafkaClient.Common;

namespace KafkaClient
{
    public class RouterConfiguration : IRouterConfiguration
    {
        public RouterConfiguration(IRetry refreshRetry = null, TimeSpan? cacheExpiration = null, IRetry sendRetry = null)
        {
            RefreshRetry = refreshRetry ?? Defaults.RefreshRetry();
            CacheExpiration = cacheExpiration ?? TimeSpan.FromMilliseconds(Defaults.CacheExpirationMilliseconds);
            SendRetry = sendRetry ?? new Retry(null, Defaults.MaxSendRetryAttempts);
        }

        /// <inheritdoc />
        public IRetry RefreshRetry { get; }

        /// <inheritdoc />
        public TimeSpan CacheExpiration { get; }

        /// <inheritdoc />
        public IRetry SendRetry { get; }

        public static class Defaults
        {
            /// <summary>
            /// The default timeout for requests made to refresh the cache
            /// </summary>
            public const int RefreshTimeoutSeconds = 200;

            /// <summary>
            /// The default maximum number of attempts made when refreshing the cache
            /// </summary>
            public const int MaxRefreshAttempts = 2;

            /// <summary>
            /// The default RefreshRetry backoff delay
            /// </summary>
            public const int RefreshDelayMilliseconds = 100;

            /// <summary>
            /// The default expiration length for <see cref="CacheExpiration"/>
            /// </summary>
            public const int CacheExpirationMilliseconds = 1000;

            /// <summary>
            /// The default attempts for <see cref="SendRetry"/>
            /// </summary>
            public const int MaxSendRetryAttempts = 3;

            public static IRetry RefreshRetry(TimeSpan? timeout = null)
            {
                return new BackoffRetry(
                    timeout ?? TimeSpan.FromSeconds(RefreshTimeoutSeconds), 
                    TimeSpan.FromMilliseconds(RefreshDelayMilliseconds), 
                    MaxRefreshAttempts,
                    true);
            }
        }
    }
}