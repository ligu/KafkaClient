using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using KafkaClient.Protocol;

namespace KafkaClient
{
    /// <summary>
    /// Contains common metadata query commands that are used by both a consumer and producer.
    /// </summary>
    internal interface IMetadataQueries : IDisposable
    {
        /// <summary>
        /// Get metadata on the given topic.
        /// </summary>
        /// <param name="topicName">The metadata on the requested topic.</param>
        /// <returns>Topic object containing the metadata on the requested topic.</returns>
        MetadataTopic GetTopicFromCache(string topicName);

        /// <summary>
        /// Get offsets for each partition from a given topic.
        /// </summary>
        /// <param name="topicName">Name of the topic to get offset information from.</param>
        /// <param name="maxOffsets"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        Task<List<OffsetTopic>> GetTopicOffsetAsync(string topicName, int maxOffsets = 2, int time = -1);
    }
}