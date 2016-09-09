using KafkaNet.Interfaces;
using KafkaNet.Protocol;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Threading.Tasks;

namespace KafkaNet
{
    /// <summary>
    /// This class implements the ManualConsumer in a thread safe manner.
    /// </summary>
    public class ManualConsumer : IManualConsumer
    {
        private readonly string _topic;
        private readonly int _partitionId;
        private readonly ProtocolGateway _gateway;
        private readonly int _maxSizeOfMessageSet;
        private readonly string _clientId;
        private ImmutableList<Message> _lastMessages;

        private static readonly TimeSpan MaxWaitTimeForKafka = TimeSpan.Zero;
        private const int UseBrokerTimestamp = -1;
        private const int NoOffsetFound = -1;

        public ManualConsumer(int partitionId, string topic, ProtocolGateway gateway, string clientId, int maxSizeOfMessageSet)
        {
            if (string.IsNullOrEmpty(topic)) throw new ArgumentNullException(nameof(topic));
            if (gateway == null) throw new ArgumentNullException(nameof(gateway));
            if (maxSizeOfMessageSet <= 0) throw new ArgumentOutOfRangeException(nameof(maxSizeOfMessageSet), "argument must be larger than zero");
            Contract.Requires(!string.IsNullOrEmpty(topic));
            Contract.Requires(gateway != null);
            Contract.Requires(maxSizeOfMessageSet > 0);

            _gateway = gateway;
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
        /// <returns></returns>
        public async Task UpdateOrCreateOffset(string consumerGroup, long offset)
        {
            if (string.IsNullOrEmpty(consumerGroup)) throw new ArgumentNullException(nameof(consumerGroup));
            if (offset < 0) throw new ArgumentOutOfRangeException(nameof(offset), "offset must be positive or zero");
            Contract.Requires(!string.IsNullOrEmpty(consumerGroup));
            Contract.Requires(offset >= 0);

            var commit = new OffsetCommit(_topic, _partitionId, offset, timeStamp: UseBrokerTimestamp);
            var request = new OffsetCommitRequest(consumerGroup, new []{ commit });
            await MakeRequestAsync(request).ConfigureAwait(false);
        }

        /// <summary>
        /// Get the max offset of the partition in the topic.
        /// </summary>
        /// <returns>The max offset, if no such offset found then returns -1</returns>
        public async Task<long> FetchLastOffset()
        {
            var request = new OffsetRequest(new Offset(_topic, _partitionId, 1));
            var response = await MakeRequestAsync(request).ConfigureAwait(false);
            var topicOffset = response.Topics.SingleOrDefault(t => t.TopicName == _topic && t.PartitionId == _partitionId);
            return topicOffset == null || topicOffset.Offsets.Count == 0 ? NoOffsetFound : topicOffset.Offsets.First();
        }

        /// <summary>
        /// Getting the offset of a specific consumer group
        /// </summary>
        /// <param name="consumerGroup">The name of the consumer group</param>
        /// <returns>The current offset of the consumerGroup</returns>
        public async Task<long> FetchOffset(string consumerGroup)
        {
            if (string.IsNullOrEmpty(consumerGroup)) throw new ArgumentNullException(nameof(consumerGroup));
            Contract.Requires(!string.IsNullOrEmpty(consumerGroup));

            var request = new OffsetFetchRequest(consumerGroup, new Topic(_topic, _partitionId));
            var response = await MakeRequestAsync(request).ConfigureAwait(false);
            var topicOffset = response.Topics.Single(t => t.TopicName == _topic && t.PartitionId == _partitionId);
            return topicOffset.Offset;
        }

        /// <summary>
        /// Getting messages from the kafka queue
        /// </summary>
        /// <param name="maxCount">The maximum amount of messages wanted. The function will return at most the wanted number of messages</param>
        /// <param name="offset">The offset to start from</param>
        /// <returns>An enumerable of the messages</returns>
        public async Task<IEnumerable<Message>> FetchMessages(int maxCount, long offset)
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
            var response = await MakeRequestAsync(request).ConfigureAwait(false);
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

        private Task<T> MakeRequestAsync<T>(IKafkaRequest<T> request) where T : class, IKafkaResponse
        {
            var context = new RequestContext(clientId: _clientId);
            return _gateway.SendProtocolRequest(request, _topic, _partitionId, context);
        }
    }
}