using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Assignment;
using KafkaClient.Common;
using KafkaClient.Protocol;

namespace KafkaClient
{
    public class Consumer : IConsumer
    {
        private readonly IRouter _router;
        private readonly bool _leaveRouterOpen;
        private readonly CancellationTokenSource _stopToken;

        public Consumer(KafkaOptions options)
            : this(new Router(options), options.ConsumerConfiguration, options.ConnectionConfiguration.Encoders, false)
        {
        }

        public Consumer(IRouter router, IConsumerConfiguration configuration = null, IImmutableDictionary<string, IMembershipEncoder> encoders = null, bool leaveRouterOpen = true)
        {
            _stopToken = new CancellationTokenSource();
            _router = router;
            _leaveRouterOpen = leaveRouterOpen;
            Configuration = configuration ?? new ConsumerConfiguration();
            _encoders = encoders ?? ImmutableDictionary<string, IMembershipEncoder>.Empty;
        }

        private readonly IImmutableDictionary<string, IMembershipEncoder> _encoders;

        public IConsumerConfiguration Configuration { get; }

        /// <inheritdoc />
        public async Task<IConsumerMessageBatch> FetchMessagesAsync(string topicName, int partitionId, long offset, int maxCount, CancellationToken cancellationToken)
        {
            if (offset < 0) throw new ArgumentOutOfRangeException(nameof(offset), offset, "must be >= 0");

            var messages = await FetchMessagesAsync(ImmutableList<Message>.Empty, topicName, partitionId, offset, maxCount, cancellationToken).ConfigureAwait(false);
            return new MessageBatch(messages, new TopicPartition(topicName, partitionId), offset, maxCount, this);
        }

        /// <inheritdoc />
        public async Task<IConsumerMessageBatch> FetchMessagesAsync(string groupId, string topicName, int partitionId, int maxCount, CancellationToken cancellationToken)
        {
            var request = new OffsetFetchRequest(groupId, new TopicPartition(topicName, partitionId));
            var response = await _router.SendAsync(request, groupId, cancellationToken).ConfigureAwait(false);
            if (!response.Errors.All(e => e.IsSuccess())) {
                throw request.ExtractExceptions(response);
            }

            return await FetchMessagesAsync(groupId, topicName, partitionId, response.Topics[0].Offset, maxCount, cancellationToken).ConfigureAwait(false);
        }

        public async Task<IConsumerMessageBatch> FetchMessagesAsync(string groupId, string topicName, int partitionId, long offset, int maxCount, CancellationToken cancellationToken)
        {
            if (offset < 0) throw new ArgumentOutOfRangeException(nameof(offset), offset, "must be >= 0");

            var messages = await FetchMessagesAsync(ImmutableList<Message>.Empty, topicName, partitionId, offset, maxCount, cancellationToken).ConfigureAwait(false);
            return new MessageBatch(messages, new TopicPartition(topicName, partitionId), offset, maxCount, this, groupId);
        }

        private async Task<ImmutableList<Message>> FetchMessagesAsync(ImmutableList<Message> existingMessages, string topicName, int partitionId, long offset, int count, CancellationToken cancellationToken)
        {
            var extracted = ExtractMessages(existingMessages, offset);
            var fetchOffset = extracted == ImmutableList<Message>.Empty
                ? offset
                : extracted[extracted.Count - 1].Offset + 1;
            var fetched = extracted.Count < count
                ? await FetchMessagesAsync(topicName, partitionId, fetchOffset, cancellationToken)
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
            for (var attempt = 0; response == null && attempt < 8; attempt++) { // at a (minimum) multiplier of 2, this results in a total factor of 256
                var request = new FetchRequest(topic, Configuration.MaxFetchServerWait, Configuration.MinFetchBytes, Configuration.MaxFetchBytes);
                try {
                    response = await _router.SendAsync(request, topicName, partitionId, cancellationToken).ConfigureAwait(false);
                } catch (BufferUnderRunException ex) {
                    if (!(Configuration.FetchByteMultiplier.GetValueOrDefault() > 1 && topic.MaxBytes > 0)) throw;
                    _router.Log.Warn(() => LogEvent.Create(ex, $"Retrying Fetch Request with multiplier {Configuration.FetchByteMultiplier}"));
                    topic = new FetchRequest.Topic(topic.TopicName, topic.PartitionId, topic.Offset, topic.MaxBytes * Configuration.FetchByteMultiplier.Value);
                }
            }
            return response.Topics.SingleOrDefault()?.Messages?.ToImmutableList() ?? ImmutableList<Message>.Empty;
        }

        private class MessageBatch : IConsumerMessageBatch
        {
            public MessageBatch(ImmutableList<Message> messages, TopicPartition partition, long offset, int maxCount, Consumer consumer, string groupId = null)
            {
                _offset = offset;
                _allMessages = messages;
                Messages = messages.Count > maxCount
                    ? messages.GetRange(0, maxCount)
                    : messages;
                _partition = partition;
                _consumer = consumer;
                _groupId = groupId;
            }

            public IImmutableList<Message> Messages { get; }
            private readonly ImmutableList<Message> _allMessages;
            private readonly TopicPartition _partition;
            private readonly Consumer _consumer;
            private readonly string _groupId;
            private long _offset;

            public async Task CommitAsync(Message lastSuccessful, CancellationToken cancellationToken)
            {
                var offset = lastSuccessful.Offset + 1;
                if (_groupId != null) {
                    await _consumer._router.CommitTopicOffsetAsync(_partition.TopicName, _partition.PartitionId, _groupId, offset, cancellationToken).ConfigureAwait(false);
                }
                _offset = offset;
            }

            public async Task<IConsumerMessageBatch> FetchNextAsync(int maxCount, CancellationToken cancellationToken)
            {
                var messages = await _consumer.FetchMessagesAsync(_allMessages, _partition.TopicName, _partition.PartitionId, _offset, maxCount, cancellationToken).ConfigureAwait(false);
                return new MessageBatch(messages, _partition, _offset, maxCount, _consumer, _groupId);
            }
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

        public async Task<IConsumerGroupMember> JoinConsumerGroupAsync(string groupId, string protocolType, IEnumerable<IMemberMetadata> metadata, CancellationToken cancellationToken, IConsumerGroupMember member = null)
        {
            if (!_encoders.ContainsKey(protocolType ?? "")) throw new ArgumentOutOfRangeException(nameof(metadata), $"ProtocolType {protocolType} is unknown");

            var protocols = metadata?.Select(m => new JoinGroupRequest.GroupProtocol(m));
            var request = new JoinGroupRequest(groupId, Configuration.GroupHeartbeat, member?.MemberId ?? "", protocolType, protocols, Configuration.GroupRebalanceTimeout);
            var response = await _router.SendAsync(request, groupId, cancellationToken, retryPolicy: Configuration.GroupCoordinationRetry).ConfigureAwait(false);
            if (!response.ErrorCode.IsSuccess()) {
                throw request.ExtractExceptions(response);
            }

            if (member != null) {
                member.OnRejoin(response);
                return member;
            }
            return new ConsumerGroupMember(this, request, response, _router.Log);
        }

        public async Task LeaveConsumerGroupAsync(string groupId, string memberId, CancellationToken cancellationToken, bool awaitResponse = true)
        {
            var request = new LeaveGroupRequest(groupId, memberId, awaitResponse);
            var response = await _router.SendAsync(request, groupId, cancellationToken, retryPolicy: Configuration.GroupCoordinationRetry).ConfigureAwait(false);
            if (awaitResponse && !response.ErrorCode.IsSuccess()) {
                throw request.ExtractExceptions(response);
            }
        }

        public async Task<ErrorResponseCode> SendHeartbeatAsync(string groupId, string memberId, int generationId, CancellationToken cancellationToken)
        {
            var request = new HeartbeatRequest(groupId, generationId, memberId);
            var response = await _router.SendAsync(request, groupId, cancellationToken, retryPolicy: Configuration.GroupCoordinationRetry).ConfigureAwait(false);
            return response.ErrorCode;
        }

        public async Task<IImmutableDictionary<string, IMemberAssignment>> SyncGroupAsync(string groupId, string memberId, int generationId, string protocolType, 
            IImmutableDictionary<string, IMemberMetadata> memberMetadata,
            IImmutableDictionary<string, IMemberAssignment> previousAssignments, 
            CancellationToken cancellationToken)
        {
            var metadata = memberMetadata.First().Value;
            var encoder = _encoders[protocolType];
            var assigner = encoder.GetAssignor(metadata.AssignmentStrategy);
            var memberAssignments = await assigner.AssignMembersAsync(_router, memberMetadata, previousAssignments, cancellationToken).ConfigureAwait(false);
            var groupAssignments = memberAssignments.Select(assignment => new SyncGroupRequest.GroupAssignment(assignment.Key, assignment.Value));

            var request = new SyncGroupRequest(groupId, generationId, memberId, groupAssignments);
            var response = await _router.SendAsync(request, groupId, cancellationToken, retryPolicy: Configuration.GroupCoordinationRetry).ConfigureAwait(false);
            if (!response.ErrorCode.IsSuccess()) {
                throw request.ExtractExceptions(response);
            }
            return memberAssignments;
        }

        public async Task<IMemberAssignment> SyncGroupAsync(string groupId, string memberId, int generationId, string protocolType, CancellationToken cancellationToken)
        {
            var request = new SyncGroupRequest(groupId, generationId, memberId);
            var response = await _router.SendAsync(request, groupId, cancellationToken, retryPolicy: Configuration.GroupCoordinationRetry).ConfigureAwait(false);
            if (!response.ErrorCode.IsSuccess()) {
                throw request.ExtractExceptions(response);
            }
            return response.MemberAssignment;
        }
    }
}