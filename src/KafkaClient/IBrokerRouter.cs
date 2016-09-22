using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Protocol;

namespace KafkaClient
{
    public interface IBrokerRouter : IDisposable
    {
        /// <summary>
        /// Get a broker for a specific topic and partitionId, from the cache.
        /// </summary>
        /// <param name="topicName">The topic name to select a broker for.</param>
        /// <param name="partitionId">The exact partition to select a broker for.</param>
        /// <returns>A broker route for the given partition of the given topic.</returns>
        /// <remarks>
        /// This function does not use any selector criteria. If the given partitionId does not exist an exception will be thrown.
        /// </remarks>
        /// <exception cref="CachedMetadataException">Thrown if the given topic or partitionId does not exist for the given topic.</exception>
        BrokerRoute GetBrokerRoute(string topicName, int partitionId);

        /// <summary>
        /// Get a broker for a given topic using the IPartitionSelector function, from the cache.
        /// </summary>
        /// <param name="topicName">The topic to retreive a broker route for.</param>
        /// <param name="key">The key used by the IPartitionSelector to collate to a consistent partition. Null value means key will be ignored in selection process.</param>
        /// <returns>A broker route for the given topic.</returns>
        /// <exception cref="CachedMetadataException">Thrown if the topic metadata does not exist in the cache.</exception>
        BrokerRoute GetBrokerRoute(string topicName, byte[] key = null);

        /// <summary>
        /// Get a broker for a specific topic and partitionId.
        /// </summary>
        /// <param name="topicName">The topic name to select a broker for.</param>
        /// <param name="partitionId">The exact partition to select a broker for.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A broker route for the given partition of the given topic.</returns>
        /// <remarks>
        /// This function does not use any selector criteria. This method will check the cache first, and if the topic and partition
        /// is missing it will initiate a call to the kafka servers, updating the cache with the resulting metadata.
        /// </remarks>
        /// <exception cref="CachedMetadataException">Thrown if the given topic or partitionId does not exist for the given topic even after a refresh.</exception>
        Task<BrokerRoute> GetBrokerRouteAsync(string topicName, int partitionId, CancellationToken cancellationToken);

        /// <summary>
        /// Returns Topic metadata for the given topic.
        /// </summary>
        /// <returns>List of Topics currently in the cache.</returns>
        /// <remarks>
        /// The topic metadata returned is from what is currently in the cache. To ensure data is not too stale, 
        /// use <see cref="GetTopicMetadataAsync(string, CancellationToken)"/>.
        /// </remarks>
        /// <exception cref="CachedMetadataException">Thrown if the topic metadata does not exist in the cache.</exception>
        MetadataTopic GetTopicMetadata(string topicName);

        /// <summary>
        /// Returns Topic metadata for each topic requested.
        /// </summary>
        /// <returns>List of Topics currently in the cache.</returns>
        /// <remarks>
        /// The topic metadata returned is from what is currently in the cache. To ensure data is not too stale,  
        /// use <see cref="GetTopicMetadataAsync(IEnumerable&lt;string&gt;, CancellationToken)"/>.
        /// </remarks>
        /// <exception cref="CachedMetadataException">Thrown if the topic metadata does not exist in the cache.</exception>
        ImmutableList<MetadataTopic> GetTopicMetadata(IEnumerable<string> topicNames);

        /// <summary>
        /// Returns all cached topic metadata.
        /// </summary>
        ImmutableList<MetadataTopic> GetTopicMetadata();

        /// <summary>
        /// Returns Topic metadata for the topic requested.
        /// </summary>
        /// <remarks>
        /// This method will check the cache first, and if the topic is missing it will initiate a call to the kafka 
        /// servers, updating the cache with the resulting metadata.
        /// </remarks>
        Task<MetadataTopic> GetTopicMetadataAsync(string topicName, CancellationToken cancellationToken);

        /// <summary>
        /// Returns Topic metadata for each topic requested.
        /// </summary>
        /// <remarks>
        /// This method will check the cache first, and for any missing topic metadata it will initiate a call to the kafka 
        /// servers, updating the cache with the resulting metadata.
        /// </remarks>
        Task<ImmutableList<MetadataTopic>> GetTopicMetadataAsync(IEnumerable<string> topicNames, CancellationToken cancellationToken);

        /// <summary>
        /// Force a call to the kafka servers to refresh metadata for the given topic.
        /// </summary>
        /// <param name="topicName">The topic name to refresh metadata for.</param>
        /// <param name="ignoreCacheExpiry">True to ignore the local cache expiry and force the call to the server.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <remarks>
        /// This method will ignore the cache and initiate a call to the kafka servers for the given topic, updating the cache with the resulting metadata.
        /// </remarks>
        Task RefreshTopicMetadataAsync(string topicName, bool ignoreCacheExpiry, CancellationToken cancellationToken);

        /// <summary>
        /// Force a call to the kafka servers to refresh metadata for all topics.
        /// </summary>
        /// <remarks>
        /// This method will ignore the cache and initiate a call to the kafka servers for all topics, updating the cache with the resulting metadata.
        /// </remarks>
        Task RefreshTopicMetadataAsync(CancellationToken cancellationToken);

        IKafkaLog Log { get; }
    }
}