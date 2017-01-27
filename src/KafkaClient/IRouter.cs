using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Assignment;
using KafkaClient.Common;
using KafkaClient.Connections;
using KafkaClient.Protocol;

namespace KafkaClient
{
    /// <summary>
    /// Provides access to Brokers for a topic (and partition) as well as topic metadata.
    /// </summary>
    public interface IRouter : IAsyncDisposable
    {
        /// <summary>
        /// Get a broker for a specific groupId, from the cache.
        /// </summary>
        /// <param name="groupId">The group to select a broker for.</param>
        /// <returns>A broker for the given group.</returns>
        /// <exception cref="CachedMetadataException">Thrown if the given groupId does not exist.</exception>
        GroupBroker GetGroupBroker(string groupId);

        /// <summary>
        /// Get a broker for a specific groupId.
        /// </summary>
        /// <param name="groupId">The group to select a broker for.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A broker for the given group.</returns>
        /// <remarks>
        /// This method will check the cache first, and if the group is missing it will initiate a call to the kafka servers, updating the cache with the resulting metadata.
        /// </remarks>
        /// <exception cref="CachedMetadataException">Thrown if the given groupId does not exist even after a refresh.</exception>
        Task<GroupBroker> GetGroupBrokerAsync(string groupId, CancellationToken cancellationToken);

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
        TopicBroker GetTopicBroker(string topicName, int partitionId);

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
        Task<TopicBroker> GetTopicBrokerAsync(string topicName, int partitionId, CancellationToken cancellationToken);

        /// <summary>
        /// Returns known member assignments for the given consumer group. If a generationId is also specified, this will also have to match.
        /// </summary>
        /// <remarks>
        /// The group assignments returned are from what is currently in the cache, since interrogating the server is impossible during rebalancing. 
        /// </remarks>
        /// <returns>An empty collection if no assignments are found.</returns>
        IImmutableDictionary<string, IMemberAssignment> GetGroupMemberAssignment(string groupId, int? generationId = null);

        /// <summary>
        /// Returns a Group metadata for the given consumer group.
        /// </summary>
        /// <remarks>
        /// The group metadata returned is from what is currently in the cache. To ensure data is not too stale, 
        /// use <see cref="GetGroupMetadataAsync(string, CancellationToken)"/>.
        /// </remarks>
        /// <exception cref="CachedMetadataException">Thrown if the group metadata does not exist in the cache.</exception>
        DescribeGroupsResponse.Group GetGroupMetadata(string groupId);

        /// <summary>
        /// Returns Group metadata for each consumer group requested.
        /// </summary>
        /// <returns>List of Groups currently in the cache.</returns>
        /// <remarks>
        /// The group metadata returned is from what is currently in the cache. To ensure data is not too stale,  
        /// use <see cref="GetGroupMetadataAsync(IEnumerable&lt;string&gt;, CancellationToken)"/>.
        /// </remarks>
        /// <exception cref="CachedMetadataException">Thrown if the group metadata does not exist in the cache.</exception>
        IImmutableList<DescribeGroupsResponse.Group> GetGroupMetadata(IEnumerable<string> groupIds);

        /// <summary>
        /// Returns a Group metadata for the given consumer group.
        /// </summary>
        /// <remarks>
        /// This method will check the cache first, and if the group is missing it will initiate a call to the kafka 
        /// servers, updating the cache with the resulting metadata.
        /// </remarks>
        Task<DescribeGroupsResponse.Group> GetGroupMetadataAsync(string groupId, CancellationToken cancellationToken);

        /// <summary>
        /// Returns Group metadata for each consumer group requested.
        /// </summary>
        /// <remarks>
        /// This method will check the cache first, and for any missing group metadata it will initiate a call to the kafka 
        /// servers, updating the cache with the resulting metadata.
        /// </remarks>
        Task<IImmutableList<DescribeGroupsResponse.Group>> GetGroupMetadataAsync(IEnumerable<string> groupIds, CancellationToken cancellationToken);

        /// <summary>
        /// Returns Topic metadata for the given topic.
        /// </summary>
        /// <returns>Topic, if currently in the cache.</returns>
        /// <remarks>
        /// The topic metadata returned is from what is currently in the cache. To ensure data is not too stale, 
        /// use <see cref="GetTopicMetadataAsync(string, CancellationToken)"/>.
        /// </remarks>
        /// <exception cref="CachedMetadataException">Thrown if the topic metadata does not exist in the cache.</exception>
        MetadataResponse.Topic GetTopicMetadata(string topicName);

        /// <summary>
        /// Returns Topic metadata for each topic requested.
        /// </summary>
        /// <returns>List of Topics currently in the cache.</returns>
        /// <remarks>
        /// The topic metadata returned is from what is currently in the cache. To ensure data is not too stale,  
        /// use <see cref="GetTopicMetadataAsync(IEnumerable&lt;string&gt;, CancellationToken)"/>.
        /// </remarks>
        /// <exception cref="CachedMetadataException">Thrown if the topic metadata does not exist in the cache.</exception>
        IImmutableList<MetadataResponse.Topic> GetTopicMetadata(IEnumerable<string> topicNames);

        /// <summary>
        /// Returns all cached topic metadata.
        /// </summary>
        IImmutableList<MetadataResponse.Topic> GetTopicMetadata();

        /// <summary>
        /// Returns Topic metadata for the topic requested.
        /// </summary>
        /// <remarks>
        /// This method will check the cache first, and if the topic is missing it will initiate a call to the kafka 
        /// servers, updating the cache with the resulting metadata.
        /// </remarks>
        Task<MetadataResponse.Topic> GetTopicMetadataAsync(string topicName, CancellationToken cancellationToken);

        /// <summary>
        /// Returns Topic metadata for each topic requested.
        /// </summary>
        /// <remarks>
        /// This method will check the cache first, and for any missing topic metadata it will initiate a call to the kafka 
        /// servers, updating the cache with the resulting metadata.
        /// </remarks>
        Task<IImmutableList<MetadataResponse.Topic>> GetTopicMetadataAsync(IEnumerable<string> topicNames, CancellationToken cancellationToken);

        /// <summary>
        /// Force a call to the kafka servers to refresh brokers for the given group.
        /// </summary>
        /// <param name="groupId">The group name to refresh metadata for.</param>
        /// <param name="ignoreCacheExpiry">True to ignore the local cache expiry and force the call to the server.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <remarks>
        /// This method will ignore the cache and initiate a call to the kafka servers for the given group, updating the cache with the resulting brokers.
        /// </remarks>
        Task RefreshGroupBrokerAsync(string groupId, bool ignoreCacheExpiry, CancellationToken cancellationToken);

        /// <summary>
        /// Force a call to the kafka servers to refresh metadata for the given group.
        /// </summary>
        /// <param name="groupId">The group name to refresh metadata for.</param>
        /// <param name="ignoreCacheExpiry">True to ignore the local cache expiry and force the call to the server.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <remarks>
        /// This method will ignore the cache and initiate a call to the kafka servers for the given group, updating the cache with the resulting metadata.
        /// </remarks>
        Task RefreshGroupMetadataAsync(string groupId, bool ignoreCacheExpiry, CancellationToken cancellationToken);

        /// <summary>
        /// Force a call to the kafka servers to refresh metadata for the given groups.
        /// </summary>
        /// <param name="groupIds">The groups to refresh metadata for.</param>
        /// <param name="ignoreCacheExpiry">True to ignore the local cache expiry and force the call to the server.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <remarks>
        /// This method will ignore the cache and initiate a call to the kafka servers for the given groups, updating the cache with the resulting metadata.
        /// </remarks>
        Task RefreshGroupMetadataAsync(IEnumerable<string> groupIds, bool ignoreCacheExpiry, CancellationToken cancellationToken);

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
        /// Force a call to the kafka servers to refresh metadata for the given topics.
        /// </summary>
        /// <param name="topicNames">The topic names to refresh metadata for.</param>
        /// <param name="ignoreCacheExpiry">True to ignore the local cache expiry and force the call to the server.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <remarks>
        /// This method will ignore the cache and initiate a call to the kafka servers for the given topics, updating the cache with the resulting metadata.
        /// </remarks>
        Task RefreshTopicMetadataAsync(IEnumerable<string> topicNames, bool ignoreCacheExpiry, CancellationToken cancellationToken);

        /// <summary>
        /// Force a call to the kafka servers to refresh metadata for all topics.
        /// </summary>
        /// <remarks>
        /// This method will ignore the cache and initiate a call to the kafka servers for all topics, updating the cache with the resulting metadata.
        /// </remarks>
        Task RefreshTopicMetadataAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Sync Groups, to populate the cache with Group Member Assignments.
        /// </summary>
        /// <exception cref="RequestException">Unless the response is successful</exception>
        Task<SyncGroupResponse> SyncGroupAsync(SyncGroupRequest request, IRequestContext context, IRetry retryPolicy, CancellationToken cancellationToken);

        /// <summary>
        /// Log for the router
        /// </summary>
        ILog Log { get; }

        /// <summary>
        /// The configuration for the connections
        /// </summary>
        IConnectionConfiguration ConnectionConfiguration { get; }

        /// <summary>
        /// The configuration for cache expiry and refresh
        /// </summary>
        IRouterConfiguration Configuration { get; }

        /// <summary>
        /// The list of currently configured connections.
        /// </summary>
        /// <remarks>
        /// Not all results are necessarily live, although they would need to have been at some point.
        /// </remarks>
        IEnumerable<IConnection> Connections { get; }

        /// <summary>
        /// Most group membership rebalancing need to happen on distinct connections. This acts to get a new connection for a given member.
        /// </summary>
        Task<IConnection> GetConnectionAsync(string groupid, string memberId, CancellationToken cancellationToken);

        /// <summary>
        /// Returns the connection back to the router, to be reused by other members as needed
        /// </summary>
        void ReturnConnection(IConnection connection);

        /// <summary>
        /// Attempt to restore or recreate the connection.
        /// Only will be attempted on disposed connections that are owned by the router.
        /// </summary>
        /// <returns>True if the connection is now live</returns>
        bool TryRestore(IConnection connection, CancellationToken cancellationToken);
    }
}