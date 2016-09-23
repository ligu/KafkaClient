using System;

namespace KafkaClient
{
    public interface ICacheConfiguration
    {
        /// <summary>
        /// The maximum time to wait when refreshing metadata.
        /// </summary>
        TimeSpan RefreshTimeout { get; }

        int MaxRetries { get; }

        /// <summary>
        /// The minimum time to cache metadata (unless explicitly forced to refresh).
        /// </summary>
        TimeSpan CacheExpiration { get; }
    }
}