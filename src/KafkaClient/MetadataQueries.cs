using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Protocol;

namespace KafkaClient
{
    /// <summary>
    /// This class provides a set of common queries that are useful for both the Consumer and Producer classes.
    /// </summary>
    public class MetadataQueries : IMetadataQueries
    {
        private readonly IBrokerRouter _brokerRouter;

        public MetadataQueries(IBrokerRouter brokerRouter)
        {
            _brokerRouter = brokerRouter;
        }

        /// <summary>
        /// Get offsets for each partition from a given topic.
        /// </summary>
        /// <param name="topic">Name of the topic to get offset information from.</param>
        /// <param name="maxOffsets"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public async Task<List<OffsetTopic>> GetTopicOffsetAsync(string topic, int maxOffsets = 2, int time = -1)
        {
            var topicMetadata = await _brokerRouter.GetTopicMetadataAsync(topic, CancellationToken.None).ConfigureAwait(false);

            //send the offset request to each partition leader
            var sendRequests = topicMetadata.Partitions
                .GroupBy(x => x.PartitionId)
                .Select(p =>
                    {
                        var route = _brokerRouter.GetBrokerRoute(topic, p.Key);
                        var request = new OffsetRequest(new Offset(topic, p.Key, time, maxOffsets));

                        return route.Connection.SendAsync(request, CancellationToken.None);
                    }).ToArray();

            await Task.WhenAll(sendRequests).ConfigureAwait(false);
            return sendRequests.SelectMany(x => x.Result.Topics).ToList();
        }

        /// <summary>
        /// Get metadata on the given topic.
        /// </summary>
        /// <param name="topic">The metadata on the requested topic.</param>
        /// <returns>Topic object containing the metadata on the requested topic.</returns>
        public MetadataTopic GetTopicFromCache(string topic)
        {
            return _brokerRouter.GetTopicMetadata(topic);
        }

        public void Dispose()
        {
            using (_brokerRouter) { }
        }
    }
}