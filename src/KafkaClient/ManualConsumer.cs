using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Protocol;

namespace KafkaClient
{
    /// <summary>
    /// This class implements the ManualConsumer in a thread safe manner.
    /// </summary>
    public class ManualConsumer : IManualConsumer
    {
        private readonly string _topic;
        private readonly int _partitionId;
        private readonly int _maxSizeOfMessageSet;
        private readonly string _clientId;
        private ImmutableList<Message> _lastMessages;
        private readonly IBrokerRouter _brokerRouter;

        private static readonly TimeSpan MaxWaitTimeForKafka = TimeSpan.Zero;
        private const int UseBrokerTimestamp = -1;
        private const int NoOffsetFound = -1;

        public ManualConsumer(int partitionId, string topic, IBrokerRouter brokerRouter, string clientId, int maxSizeOfMessageSet)
        {
            if (string.IsNullOrEmpty(topic)) throw new ArgumentNullException(nameof(topic));
            if (brokerRouter == null) throw new ArgumentNullException(nameof(brokerRouter));
            if (maxSizeOfMessageSet <= 0) throw new ArgumentOutOfRangeException(nameof(maxSizeOfMessageSet), "argument must be larger than zero");
            Contract.Requires(!string.IsNullOrEmpty(topic));
            Contract.Requires(brokerRouter != null);
            Contract.Requires(maxSizeOfMessageSet > 0);

            _brokerRouter = brokerRouter;
            _partitionId = partitionId;
            _topic = topic;
            _clientId = clientId;
            _maxSizeOfMessageSet = maxSizeOfMessageSet;
        }

        /// <summary>
        /// Updating the cosumerGroup's offset for the partition in topic
        /// </summary>
        /// <param name="consumerGroup">The consumer group</param>
        /// <param name="offset">The new offset. must be larger than or equal to zero</param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task UpdateOrCreateOffsetAsync(string consumerGroup, long offset, CancellationToken cancellationToken)
        {
            if (string.IsNullOrEmpty(consumerGroup)) throw new ArgumentNullException(nameof(consumerGroup));
            if (offset < 0) throw new ArgumentOutOfRangeException(nameof(offset), "offset must be positive or zero");
            Contract.Requires(!string.IsNullOrEmpty(consumerGroup));
            Contract.Requires(offset >= 0);

            var commit = new OffsetCommit(_topic, _partitionId, offset, timeStamp: UseBrokerTimestamp);
            var request = new OffsetCommitRequest(consumerGroup, new []{ commit });
            await MakeRequestAsync(request, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Get the max offset of the partition in the topic.
        /// </summary>
        /// <returns>The max offset, if no such offset found then returns -1</returns>
        public async Task<long> FetchLastOffsetAsync(CancellationToken cancellationToken)
        {
            var request = new OffsetRequest(new Offset(_topic, _partitionId));
            var response = await MakeRequestAsync(request, cancellationToken).ConfigureAwait(false);
            var topicOffset = response.Topics.SingleOrDefault(t => t.TopicName == _topic && t.PartitionId == _partitionId);
            return topicOffset == null || topicOffset.Offsets.Count == 0 ? NoOffsetFound : topicOffset.Offsets.First();
        }

        /// <summary>
        /// Getting the offset of a specific consumer group
        /// </summary>
        /// <param name="consumerGroup">The name of the consumer group</param>
        /// <param name="cancellationToken"></param>
        /// <returns>The current offset of the consumerGroup</returns>
        public async Task<long> FetchOffsetAsync(string consumerGroup, CancellationToken cancellationToken)
        {
            if (string.IsNullOrEmpty(consumerGroup)) throw new ArgumentNullException(nameof(consumerGroup));
            Contract.Requires(!string.IsNullOrEmpty(consumerGroup));

            var request = new OffsetFetchRequest(consumerGroup, new Topic(_topic, _partitionId));
            var response = await MakeRequestAsync(request, cancellationToken).ConfigureAwait(false);
            var topicOffset = response.Topics.Single(t => t.TopicName == _topic && t.PartitionId == _partitionId);
            return topicOffset.Offset;
        }

        /// <summary>
        /// Getting messages from the kafka queue
        /// </summary>
        /// <param name="maxCount">The maximum amount of messages wanted. The function will return at most the wanted number of messages</param>
        /// <param name="offset">The offset to start from</param>
        /// <param name="cancellationToken"></param>
        /// <returns>An enumerable of the messages</returns>
        public async Task<IEnumerable<Message>> FetchMessagesAsync(int maxCount, long offset, CancellationToken cancellationToken)
        {
            if (offset < 0) throw new ArgumentOutOfRangeException(nameof(offset), "offset must be positive or zero");

            // Checking if the last fetch task has the wanted batch of messages
            if (_lastMessages != null) {
                var startIndex = _lastMessages.FindIndex(m => m.Offset == offset);
                var containsAllMessage = startIndex != -1 && startIndex + maxCount <= _lastMessages.Count;
                if (containsAllMessage) {
                    return _lastMessages.GetRange(startIndex, maxCount);
                }
            }

            // If we arrived here, then we need to make a new fetch request and work with it
            var fetch = new Fetch(_topic, _partitionId, offset, _maxSizeOfMessageSet);
            var request = new FetchRequest(fetch, MaxWaitTimeForKafka, 0);
            var response = await MakeRequestAsync(request, cancellationToken).ConfigureAwait(false);
            var topic = response.Topics.SingleOrDefault();

            if (topic?.Messages?.Count == 0) {
                _lastMessages = null;
                return topic.Messages;
            }

            // Saving the last consumed offset and Returning the wanted amount
            _lastMessages = topic?.Messages;
            var messagesToReturn = topic?.Messages?.Take(maxCount);
            return messagesToReturn;
        }

        private Task<T> MakeRequestAsync<T>(IRequest<T> request, CancellationToken cancellationToken) where T : class, IResponse
        {
            return _brokerRouter.SendAsync(request, _topic, _partitionId, cancellationToken, new RequestContext(clientId: _clientId));
        }
    }
}