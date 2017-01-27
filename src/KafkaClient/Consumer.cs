using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Assignment;
using KafkaClient.Common;
using KafkaClient.Connections;
using KafkaClient.Protocol;

namespace KafkaClient
{
    public class Consumer : IConsumer
    {
        private int _disposeCount = 0;
        private readonly TaskCompletionSource<bool> _disposePromise = new TaskCompletionSource<bool>();
        private readonly bool _leaveRouterOpen;

        public Consumer(IRouter router, IConsumerConfiguration configuration = null, IImmutableDictionary<string, IMembershipEncoder> encoders = null, bool leaveRouterOpen = true)
        {
            Router = router;
            _leaveRouterOpen = leaveRouterOpen;
            Configuration = configuration ?? new ConsumerConfiguration();
            Encoders = encoders ?? ConnectionConfiguration.Defaults.Encoders();
        }

        public IImmutableDictionary<string, IMembershipEncoder> Encoders { get; }

        public IConsumerConfiguration Configuration { get; }

        public IRouter Router { get; }

        /// <inheritdoc />
        public async Task<IMessageBatch> FetchBatchAsync(string topicName, int partitionId, long offset, CancellationToken cancellationToken, int? batchSize = null)
        {
            if (_disposeCount > 0) throw new ObjectDisposedException(nameof(Consumer));
            if (offset < 0) throw new ArgumentOutOfRangeException(nameof(offset), offset, "must be >= 0");

            var count = batchSize.GetValueOrDefault(Configuration.BatchSize);
            var messages = await FetchBatchAsync(ImmutableList<Message>.Empty, topicName, partitionId, offset, count, cancellationToken).ConfigureAwait(false);
            return new MessageBatch(messages, new TopicPartition(topicName, partitionId), offset, count, this);
        }

        /// <inheritdoc />
        public async Task<IMessageBatch> FetchBatchAsync(string groupId, string memberId, int generationId, string topicName, int partitionId, CancellationToken cancellationToken, int? batchSize = null)
        {
            if (_disposeCount > 0) throw new ObjectDisposedException(nameof(Consumer));

            var request = new OffsetFetchRequest(groupId, new TopicPartition(topicName, partitionId));
            var response = await Router.SendAsync(request, groupId, cancellationToken).ConfigureAwait(false);
            if (!response.Errors.All(e => e.IsSuccess())) {
                throw request.ExtractExceptions(response);
            }

            return await FetchBatchAsync(groupId, memberId, generationId, topicName, partitionId, response.Topics[0].Offset + 1, cancellationToken, batchSize).ConfigureAwait(false);
        }

        public async Task<IMessageBatch> FetchBatchAsync(string groupId, string memberId, int generationId, string topicName, int partitionId, long offset, CancellationToken cancellationToken, int? batchSize = null)
        {
            if (_disposeCount > 0) throw new ObjectDisposedException(nameof(Consumer));
            if (offset < 0) throw new ArgumentOutOfRangeException(nameof(offset), offset, "must be >= 0");

            var count = batchSize.GetValueOrDefault(Configuration.BatchSize);
            var messages = await FetchBatchAsync(ImmutableList<Message>.Empty, topicName, partitionId, offset, count, cancellationToken).ConfigureAwait(false);
            return new MessageBatch(messages, new TopicPartition(topicName, partitionId), offset, count, this, groupId, memberId, generationId);
        }

        private async Task<ImmutableList<Message>> FetchBatchAsync(ImmutableList<Message> existingMessages, string topicName, int partitionId, long offset, int count, CancellationToken cancellationToken)
        {
            var extracted = ExtractMessages(existingMessages, offset);
            var fetchOffset = extracted == ImmutableList<Message>.Empty
                ? offset
                : extracted[extracted.Count - 1].Offset + 1;
            var fetched = extracted.Count < count
                ? await FetchMessagesAsync(topicName, partitionId, fetchOffset, cancellationToken).ConfigureAwait(false)
                : ImmutableList<Message>.Empty;

            if (extracted == ImmutableList<Message>.Empty) return fetched;
            if (fetched == ImmutableList<Message>.Empty) return extracted;
            return extracted.AddRange(fetched);
        }

        private static ImmutableList<Message> ExtractMessages(ImmutableList<Message> existingMessages, long offset)
        {
            var localIndex = existingMessages.FindIndex(m => m.Offset == offset);
            if (localIndex == 0) return existingMessages;
            if (0 < localIndex) {
                return existingMessages.GetRange(localIndex, existingMessages.Count - (localIndex + 1));
            }
            return ImmutableList<Message>.Empty;
        }

        private async Task<ImmutableList<Message>> FetchMessagesAsync(string topicName, int partitionId, long offset, CancellationToken cancellationToken)
        {
            var topic = new FetchRequest.Topic(topicName, partitionId, offset, Configuration.MaxPartitionFetchBytes);
            FetchResponse response = null;
            for (var attempt = 1; response == null && attempt <= 12; attempt++) { // at a (minimum) multiplier of 2, this results in a total factor of 256
                var request = new FetchRequest(topic, Configuration.MaxFetchServerWait, Configuration.MinFetchBytes, Configuration.MaxFetchBytes);
                try {
                    response = await Router.SendAsync(request, topicName, partitionId, cancellationToken).ConfigureAwait(false);
                } catch (BufferUnderRunException ex) {
                    if (!Configuration.FetchByteMultiplier.HasValue || Configuration.FetchByteMultiplier.GetValueOrDefault() <= 1) throw;
                    var maxBytes = topic.MaxBytes * Configuration.FetchByteMultiplier.Value;
                    Router.Log.Warn(() => LogEvent.Create(ex, $"Retrying Fetch Request with multiplier {Math.Pow(Configuration.FetchByteMultiplier.Value, attempt)}, {topic.MaxBytes} -> {maxBytes}"));
                    topic = new FetchRequest.Topic(topic.TopicName, topic.PartitionId, topic.Offset, maxBytes);
                }
            }
            return response?.Topics?.SingleOrDefault()?.Messages?.ToImmutableList() ?? ImmutableList<Message>.Empty;
        }

        private class MessageBatch : IMessageBatch
        {
            public MessageBatch(ImmutableList<Message> messages, TopicPartition partition, long offset, int batchSize, Consumer consumer, string groupId = null, string memberId = null, int generationId = -1)
            {
                _offsetMarked = offset;
                _offsetCommitted = offset;
                _allMessages = messages;
                _batchSize = batchSize;
                Messages = messages.Count > batchSize
                    ? messages.GetRange(0, batchSize)
                    : messages;
                _partition = partition;
                _consumer = consumer;
                _groupId = groupId;
                _memberId = memberId;
                _generationId = generationId;
            }

            public IImmutableList<Message> Messages { get; }
            private readonly int _batchSize;
            private readonly ImmutableList<Message> _allMessages;
            private readonly TopicPartition _partition;
            private readonly Consumer _consumer;
            private readonly string _groupId;
            private readonly string _memberId;
            private readonly int _generationId;
            private long _offsetMarked;
            private long _offsetCommitted;
            private int _disposeCount;

            public async Task<IMessageBatch> FetchNextAsync(CancellationToken cancellationToken)
            {
                var offset = await CommitMarkedAsync(cancellationToken).ConfigureAwait(false);
                var messages = await _consumer.FetchBatchAsync(_allMessages, _partition.TopicName, _partition.PartitionId, offset, _batchSize, cancellationToken).ConfigureAwait(false);
                return new MessageBatch(messages, _partition, offset, _batchSize, _consumer);
            }

            public void MarkSuccessful(Message message)
            {
                if (_disposeCount > 0) throw new ObjectDisposedException($"The topic/{_partition.TopicName}/partition/{_partition.PartitionId} batch is disposed.");
                var offset = message.Offset + 1;
                if (_offsetMarked > offset) throw new ArgumentOutOfRangeException(nameof(message), $"Marked offset is {_offsetMarked}, cannot mark previous offset of {offset}.");
                _offsetMarked = message.Offset + 1;
            }

            public async Task<long> CommitMarkedAsync(CancellationToken cancellationToken)
            {
                if (_disposeCount > 0) throw new ObjectDisposedException($"The topic/{_partition.TopicName}/partition/{_partition.PartitionId} batch is disposed.");

                var offset = _offsetMarked;
                var committed = _offsetCommitted;
                if (offset <= committed) return committed;

                if (_groupId != null && _memberId != null) {
                    var request = new OffsetCommitRequest(_groupId, new[] { new OffsetCommitRequest.Topic(_partition.TopicName, _partition.PartitionId, offset) }, _memberId, _generationId);
                    await _consumer.Router.SendAsync(request, _partition.TopicName, _partition.PartitionId, cancellationToken).ConfigureAwait(false);
                }
                _offsetCommitted = offset;
                return offset;
            }

            public void Dispose()
            {
                if (Interlocked.Increment(ref _disposeCount) != 1) return;
                OnDisposed?.Invoke();
            }

            public Action OnDisposed { get; set; }
        }

        public async Task DisposeAsync()
        {
            if (Interlocked.Increment(ref _disposeCount) != 1) {
                await _disposePromise.Task;
                return;
            }

            try {
                Router.Log.Debug(() => LogEvent.Create("Disposing Consumer"));
                if (_leaveRouterOpen) return;
                await Router.DisposeAsync();
            } finally {
                _disposePromise.TrySetResult(true);
            }
        }

        public void Dispose()
        {
#pragma warning disable 4014
            // trigger, and set the promise appropriately
            DisposeAsync();
#pragma warning restore 4014
        }

        public async Task<IConsumerMember> JoinGroupAsync(string groupId, string protocolType, IEnumerable<IMemberMetadata> metadata, CancellationToken cancellationToken)
        {
            if (_disposeCount > 0) throw new ObjectDisposedException(nameof(Consumer));
            if (!Encoders.ContainsKey(protocolType ?? "")) throw new ArgumentOutOfRangeException(nameof(metadata), $"ProtocolType {protocolType} is unknown");

            var protocols = metadata?.Select(m => new JoinGroupRequest.GroupProtocol(m));
            var request = new JoinGroupRequest(groupId, Configuration.GroupHeartbeat, null, protocolType, protocols, Configuration.GroupRebalanceTimeout);
            var response = await Router.SendAsync(request, groupId, cancellationToken, new RequestContext(protocolType: protocolType), Configuration.GroupCoordinationRetry).ConfigureAwait(false);
            if (!response.ErrorCode.IsSuccess()) {
                throw request.ExtractExceptions(response);
            }

            return new ConsumerMember(this, request, response);
        }
    }
}