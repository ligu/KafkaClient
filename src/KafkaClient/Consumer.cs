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
    public class Consumer : IConsumer, IDisposable
    {
        private readonly IBrokerRouter _brokerRouter;
        private readonly bool _leaveRouterOpen;
        private readonly CancellationTokenSource _stopToken;

        public Consumer(KafkaOptions options)
            : this(new BrokerRouter(options), options.ConsumerConfiguration, false)
        {
        }

        public Consumer(IBrokerRouter brokerRouter, IConsumerConfiguration configuration = null, bool leaveRouterOpen = true)
        {
            _stopToken = new CancellationTokenSource();
            _brokerRouter = brokerRouter;
            _leaveRouterOpen = leaveRouterOpen;
            Configuration = configuration ?? new ConsumerConfiguration();
            _localMessages = ImmutableList<Message>.Empty;
        }

        public IConsumerConfiguration Configuration { get; }

        private ImmutableList<Message> _localMessages;

        /// <inheritdoc />
        public async Task<IImmutableList<Message>> FetchMessagesAsync(string topicName, int partitionId, long offset, int maxCount, CancellationToken cancellationToken)
        {
            if (offset < 0) throw new ArgumentOutOfRangeException(nameof(offset), offset, "must be >= 0");

            // Previously fetched messages may contain everything we need
            var localIndex = _localMessages.FindIndex(m => m.Offset == offset);
            if (0 <= localIndex && localIndex + maxCount <= _localMessages.Count) return _localMessages.GetRange(localIndex, maxCount);

            var localCount = (0 <= localIndex && localIndex < _localMessages.Count) ? _localMessages.Count - localIndex : 0;
            var request = new FetchRequest(new FetchRequest.Topic(topicName, partitionId, offset + localCount, Configuration.MaxPartitionFetchBytes), 
                Configuration.MaxFetchServerWait, Configuration.MinFetchBytes, Configuration.MaxFetchBytes);
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

        public void Dispose()
        {
            using (_stopToken) {
                if (_leaveRouterOpen) return;
                using (_brokerRouter)
                {
                }
            }
        }
    }
}