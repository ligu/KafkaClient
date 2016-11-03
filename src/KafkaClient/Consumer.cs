using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Protocol;

namespace KafkaClient
{
    /// <summary>
    /// Simple consumer with access to a single topic
    /// </summary>
    public class Consumer : IConsumer
    {
        private readonly IBrokerRouter _brokerRouter;
        private readonly IConsumerConfiguration _configuration;

        public Consumer(IBrokerRouter brokerRouter, IConsumerConfiguration configuration = null)
        {
            _brokerRouter = brokerRouter;
            _configuration = configuration ?? new ConsumerConfiguration();
            _localMessages = ImmutableList<Message>.Empty;
        }

        private ImmutableList<Message> _localMessages;

        /// <inheritdoc />
        public async Task<IImmutableList<Message>> FetchMessagesAsync(string topicName, int partitionId, long offset, int maxCount, CancellationToken cancellationToken)
        {
            if (offset < 0) throw new ArgumentOutOfRangeException(nameof(offset), offset, "must be >= 0");

            // Previously fetched messages may contain everything we need
            var localIndex = _localMessages.FindIndex(m => m.Offset == offset);
            if (0 <= localIndex && localIndex + maxCount <= _localMessages.Count) return _localMessages.GetRange(localIndex, maxCount);

            var localCount = (0 <= localIndex && localIndex < _localMessages.Count) ? _localMessages.Count - localIndex : 0;
            var request = new FetchRequest(new FetchRequest.Topic(topicName, partitionId, offset + localCount, _configuration.MaxFetchBytes), _configuration.MaxServerWait);
            var response = await _brokerRouter.SendAsync(request, topicName, partitionId, cancellationToken).ConfigureAwait(false);
            var topic = response.Topics.SingleOrDefault();

            if (topic?.Messages?.Count == 0) return ImmutableList<Message>.Empty;

            if (localCount > 0) {
                // Previously fetched messages contain some of what we need, so append
                _localMessages = (ImmutableList<Message>)_localMessages.AddNotNullRange(topic?.Messages);
            } else {
                localIndex = 0;
                _localMessages = topic?.Messages?.ToImmutableList() ?? ImmutableList<Message>.Empty;
            }
            return _localMessages.GetRange(localIndex, Math.Min(maxCount, _localMessages.Count));
        }
    }
}