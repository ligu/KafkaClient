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
        /// Select a broker for a specific topic and partitionId.
        /// </summary>
        /// <param name="topicName">The topic name to select a broker for.</param>
        /// <param name="partitionId">The exact partition to select a broker for.</param>
        /// <returns>A broker route for the given partition of the given topic.</returns>
        /// <remarks>
        /// This function does not use any selector criteria.  If the given partitionId does not exist an exception will be thrown.
        /// </remarks>
        /// <exception cref="CachedMetadataException">Thrown if the given topic or partitionId does not exist for the given topic.</exception>
        BrokerRoute SelectBrokerRouteFromLocalCache(string topicName, int partitionId);

        /// <summary>
        /// Select a broker for a given topic using the IPartitionSelector function.
        /// </summary>
        /// <param name="topicName">The topic to retreive a broker route for.</param>
        /// <param name="key">The key used by the IPartitionSelector to collate to a consistent partition. Null value means key will be ignored in selection process.</param>
        /// <returns>A broker route for the given topic.</returns>
        /// <exception cref="CachedMetadataException">Thrown if the topic metadata does not exist in the cache.</exception>
        BrokerRoute SelectBrokerRouteFromLocalCache(string topicName, byte[] key = null);

        /// <summary>
        /// Returns Topic metadata for the given topic.
        /// </summary>
        /// <returns>List of Topics currently in the cache.</returns>
        /// <remarks>
        /// The topic metadata returned is from what is currently in the cache. To ensure data is not too stale, call 
        /// <see cref="RefreshMissingTopicMetadataAsync(string, CancellationToken)"/> first or 
        /// use <see cref="GetTopicMetadataAsync(string, CancellationToken)"/>.
        /// </remarks>
        /// <exception cref="CachedMetadataException">Thrown if the topic metadata does not exist in the cache.</exception>
        MetadataTopic GetTopicMetadataFromLocalCache(string topicName);

        /// <summary>
        /// Returns Topic metadata for each topic requested.
        /// </summary>
        /// <returns>List of Topics currently in the cache.</returns>
        /// <remarks>
        /// The topic metadata returned is from what is currently in the cache. To ensure data is not too stale, call 
        /// <see cref="RefreshMissingTopicMetadataAsync(IEnumerable&lt;string&gt;, CancellationToken)"/> first or 
        /// use <see cref="GetTopicMetadataAsync(IEnumerable&lt;string&gt;, CancellationToken)"/>.
        /// </remarks>
        /// <exception cref="CachedMetadataException">Thrown if the topic metadata does not exist in the cache.</exception>
        ImmutableList<MetadataTopic> GetTopicMetadataFromLocalCache(IEnumerable<string> topicNames);

        /// <summary>
        /// Returns all cached topic metadata.
        /// </summary>
        ImmutableList<MetadataTopic> GetTopicMetadataFromLocalCache();

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
        /// Refresh metadata for the given topic if it isn't present in the cache.
        /// </summary>
        /// <remarks>
        /// This method will check the cache first, and if it doesn't find the topic will initiate a call to the kafka servers, updating the cache with the resulting metadata.
        /// </remarks>
        Task<bool> RefreshMissingTopicMetadataAsync(string topicName, CancellationToken cancellationToken);

        /// <summary>
        /// Refresh metadata for the given topics which aren't present in the cache.
        /// </summary>
        /// <remarks>
        /// This method will check the cache first, and for the topics it doesn't find it will initiate a call to the kafka servers, updating the cache with the resulting metadata.
        /// </remarks>
        Task<bool> RefreshMissingTopicMetadataAsync(IEnumerable<string> topicNames, CancellationToken cancellationToken);

        /// <summary>
        /// Force a call to the kafka servers to refresh metadata for the given topic.
        /// </summary>
        /// <remarks>
        /// This method will ignore the cache and initiate a call to the kafka servers for the given topic, updating the cache with the resulting metadata.
        /// </remarks>
        Task RefreshTopicMetadataAsync(string topicName, CancellationToken cancellationToken);

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