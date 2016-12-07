using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Connections;
using KafkaClient.Protocol;
using KafkaClient.Protocol.Types;

namespace KafkaClient
{
    /// <summary>
    /// Simple consumer with access to a single topic
    /// </summary>
    public class Consumer : IConsumer, IDisposable
    {
        private readonly IRouter _router;
        private readonly bool _leaveRouterOpen;
        private readonly CancellationTokenSource _stopToken;

        public Consumer(KafkaOptions options)
            : this(new Router(options), options.ConsumerConfiguration, options.ConnectionConfiguration.Encoders, false)
        {
        }

        public Consumer(IRouter router, IConsumerConfiguration configuration = null, IImmutableDictionary<string, IProtocolTypeEncoder> encoders = null, bool leaveRouterOpen = true)
        {
            _stopToken = new CancellationTokenSource();
            _router = router;
            _leaveRouterOpen = leaveRouterOpen;
            Configuration = configuration ?? new ConsumerConfiguration();
            _localMessages = ImmutableList<Message>.Empty;
            Encoders = encoders ?? ImmutableDictionary<string, IProtocolTypeEncoder>.Empty;
        }

        public IImmutableDictionary<string, IProtocolTypeEncoder> Encoders { get; }

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
            var response = await _router.SendAsync(request, topicName, partitionId, cancellationToken).ConfigureAwait(false);
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
                using (_router)
                {
                }
            }
        }

        public async Task<IConsumerGroupMember> JoinConsumerGroupAsync(string groupId, IMemberMetadata metadata, CancellationToken cancellationToken, string memberId = "")
        {
            var request = new JoinGroupRequest(groupId, Configuration.GroupHeartbeat, memberId, metadata.ProtocolType, new [] { new JoinGroupRequest.GroupProtocol(metadata.ProtocolType, metadata) }, Configuration.GroupRebalanceTimeout);
            foreach (var connection in _router.Connections) {
                try {
                    var response = await connection.SendAsync(request, cancellationToken);

                    return new ConsumerGroupMember(this, groupId, response.MemberId, response.LeaderId, response.GenerationId);
                } catch (ConnectionException ex) {
                    _router.Log.Info(() => LogEvent.Create(ex, $"Skipping connection that failed: {ex.Endpoint}"));
                }
            }

            throw new ConnectionException("None of the provided Kafka servers are available to join.");
        }
    }
}