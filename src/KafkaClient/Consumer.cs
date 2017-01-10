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
        private readonly bool _leaveRouterOpen;
        private readonly CancellationTokenSource _stopToken;

        public Consumer(KafkaOptions options)
            : this(new Router(options), options.ConsumerConfiguration, options.ConnectionConfiguration.Encoders, false)
        {
        }

        public Consumer(IRouter router, IConsumerConfiguration configuration = null, IImmutableDictionary<string, IMembershipEncoder> encoders = null, bool leaveRouterOpen = true)
        {
            _stopToken = new CancellationTokenSource();
            Router = router;
            _leaveRouterOpen = leaveRouterOpen;
            Configuration = configuration ?? new ConsumerConfiguration();
            _encoders = encoders ?? ImmutableDictionary<string, IMembershipEncoder>.Empty;
        }

        private readonly IImmutableDictionary<string, IMembershipEncoder> _encoders;

        public IConsumerConfiguration Configuration { get; }

        public IRouter Router { get; }

        /// <inheritdoc />
        public async Task<IConsumerMessageBatch> FetchBatchAsync(string topicName, int partitionId, long offset, int maxCount, CancellationToken cancellationToken)
        {
            if (offset < 0) throw new ArgumentOutOfRangeException(nameof(offset), offset, "must be >= 0");

            var messages = await FetchBatchAsync(ImmutableList<Message>.Empty, topicName, partitionId, offset, maxCount, cancellationToken).ConfigureAwait(false);
            return new MessageBatch(messages, new TopicPartition(topicName, partitionId), offset, maxCount, this);
        }

        /// <inheritdoc />
        public async Task<IConsumerMessageBatch> FetchBatchAsync(string groupId, string memberId, int generationId, string topicName, int partitionId, int maxCount, CancellationToken cancellationToken)
        {
            var request = new OffsetFetchRequest(groupId, new TopicPartition(topicName, partitionId));
            var response = await Router.SendAsync(request, groupId, cancellationToken).ConfigureAwait(false);
            if (!response.Errors.All(e => e.IsSuccess())) {
                throw request.ExtractExceptions(response);
            }

            return await FetchBatchAsync(groupId, memberId, generationId, topicName, partitionId, response.Topics[0].Offset + 1, maxCount, cancellationToken).ConfigureAwait(false);
        }

        public async Task<IConsumerMessageBatch> FetchBatchAsync(string groupId, string memberId, int generationId, string topicName, int partitionId, long offset, int maxCount, CancellationToken cancellationToken)
        {
            if (offset < 0) throw new ArgumentOutOfRangeException(nameof(offset), offset, "must be >= 0");

            var messages = await FetchBatchAsync(ImmutableList<Message>.Empty, topicName, partitionId, offset, maxCount, cancellationToken).ConfigureAwait(false);
            return new MessageBatch(messages, new TopicPartition(topicName, partitionId), offset, maxCount, this, groupId, memberId, generationId);
        }

        private async Task<ImmutableList<Message>> FetchBatchAsync(ImmutableList<Message> existingMessages, string topicName, int partitionId, long offset, int count, CancellationToken cancellationToken)
        {
            var extracted = ExtractMessages(existingMessages, offset);
            var fetchOffset = extracted == ImmutableList<Message>.Empty
                ? offset
                : extracted[extracted.Count - 1].Offset + 1;
            var fetched = extracted.Count < count
                ? await FetchBatchAsync(topicName, partitionId, fetchOffset, cancellationToken)
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

        private async Task<ImmutableList<Message>> FetchBatchAsync(string topicName, int partitionId, long offset, CancellationToken cancellationToken)
        {
            var topic = new FetchRequest.Topic(topicName, partitionId, offset, Configuration.MaxPartitionFetchBytes);
            FetchResponse response = null;
            for (var attempt = 0; response == null && attempt < 8; attempt++) { // at a (minimum) multiplier of 2, this results in a total factor of 256
                var request = new FetchRequest(topic, Configuration.MaxFetchServerWait, Configuration.MinFetchBytes, Configuration.MaxFetchBytes);
                try {
                    response = await Router.SendAsync(request, topicName, partitionId, cancellationToken).ConfigureAwait(false);
                } catch (BufferUnderRunException ex) {
                    if (!(Configuration.FetchByteMultiplier.GetValueOrDefault() > 1 && topic.MaxBytes > 0)) throw;
                    Router.Log.Warn(() => LogEvent.Create(ex, $"Retrying Fetch Request with multiplier {Configuration.FetchByteMultiplier}"));
                    topic = new FetchRequest.Topic(topic.TopicName, topic.PartitionId, topic.Offset, topic.MaxBytes * Configuration.FetchByteMultiplier.Value);
                }
            }
            return response.Topics.SingleOrDefault()?.Messages?.ToImmutableList() ?? ImmutableList<Message>.Empty;
        }

        private class MessageBatch : IConsumerMessageBatch
        {
            public MessageBatch(ImmutableList<Message> messages, TopicPartition partition, long offset, int maxCount, Consumer consumer, string groupId = null, string memberId = null, int generationId = -1)
            {
                _offsetMarked = offset;
                _offsetCommitted = offset;
                _allMessages = messages;
                Messages = messages.Count > maxCount
                    ? messages.GetRange(0, maxCount)
                    : messages;
                _partition = partition;
                _consumer = consumer;
                _groupId = groupId;
                _memberId = memberId;
                _generationId = generationId;
            }

            public IImmutableList<Message> Messages { get; }
            private readonly ImmutableList<Message> _allMessages;
            private readonly TopicPartition _partition;
            private readonly Consumer _consumer;
            private readonly string _groupId;
            private readonly string _memberId;
            private readonly int _generationId;
            private long _offsetMarked;
            private long _offsetCommitted;
            private int _disposeCount = 0;

            public async Task<IConsumerMessageBatch> FetchNextAsync(int maxCount, CancellationToken cancellationToken)
            {
                var offset = await CommitMarkedAsync(cancellationToken);
                var messages = await _consumer.FetchBatchAsync(_allMessages, _partition.TopicName, _partition.PartitionId, offset, maxCount, cancellationToken).ConfigureAwait(false);
                return new MessageBatch(messages, _partition, offset, maxCount, _consumer);
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
                    await _consumer.Router.SendAsync(request, _partition.TopicName, _partition.PartitionId, _groupId, cancellationToken).ConfigureAwait(false);
                }
                _offsetCommitted = offset;
                return offset;
            }

            public void Dispose()
            {
                Interlocked.Increment(ref _disposeCount);
            }
        }

        public void Dispose()
        {
            using (_stopToken) {
                if (_leaveRouterOpen) return;
                using (Router)
                {
                }
            }
        }

        public async Task<IConsumerGroupMember> JoinConsumerGroupAsync(string groupId, string protocolType, IEnumerable<IMemberMetadata> metadata, CancellationToken cancellationToken, IConsumerGroupMember member = null)
        {
            if (!_encoders.ContainsKey(protocolType ?? "")) throw new ArgumentOutOfRangeException(nameof(metadata), $"ProtocolType {protocolType} is unknown");

            var protocols = metadata?.Select(m => new JoinGroupRequest.GroupProtocol(m));
            var request = new JoinGroupRequest(groupId, Configuration.GroupHeartbeat, member?.MemberId ?? "", protocolType, protocols, Configuration.GroupRebalanceTimeout);
            var response = await Router.SendAsync(request, groupId, cancellationToken, retryPolicy: Configuration.GroupCoordinationRetry).ConfigureAwait(false);
            if (!response.ErrorCode.IsSuccess()) {
                throw request.ExtractExceptions(response);
            }

            if (member != null) {
                member.OnJoinGroup(response);
                return member;
            }
            return new ConsumerGroupMember(this, request, response, Router.Log);
        }

        public async Task LeaveConsumerGroupAsync(string groupId, string memberId, CancellationToken cancellationToken, bool awaitResponse = true)
        {
            var request = new LeaveGroupRequest(groupId, memberId, awaitResponse);
            var response = await Router.SendAsync(request, groupId, cancellationToken, retryPolicy: Configuration.GroupCoordinationRetry).ConfigureAwait(false);
            if (awaitResponse && !response.ErrorCode.IsSuccess()) {
                throw request.ExtractExceptions(response);
            }
        }

        public async Task<ErrorResponseCode> SendHeartbeatAsync(string groupId, string memberId, int generationId, CancellationToken cancellationToken)
        {
            var request = new HeartbeatRequest(groupId, generationId, memberId);
            var response = await Router.SendAsync(request, groupId, cancellationToken, retryPolicy: Configuration.GroupCoordinationRetry).ConfigureAwait(false);
            return response.ErrorCode;
        }

        public async Task<IImmutableDictionary<string, IMemberAssignment>> SyncGroupAsync(string groupId, string memberId, int generationId, string protocolType, 
            IImmutableDictionary<string, IMemberMetadata> memberMetadata,
            IImmutableDictionary<string, IMemberAssignment> currentAssignments, 
            CancellationToken cancellationToken)
        {
            var memberAssignments = await AssignMembersAsync(groupId, protocolType, memberMetadata, currentAssignments, cancellationToken);
            var groupAssignments = memberAssignments.Select(assignment => new SyncGroupRequest.GroupAssignment(assignment.Key, assignment.Value));

            var request = new SyncGroupRequest(groupId, generationId, memberId, groupAssignments);
            var response = await Router.SendAsync(request, groupId, cancellationToken, retryPolicy: Configuration.GroupCoordinationRetry, context: new RequestContext(protocolType: protocolType)).ConfigureAwait(false);
            if (!response.ErrorCode.IsSuccess()) {
                throw request.ExtractExceptions(response);
            }
            return memberAssignments;
        }

        private async Task<IImmutableDictionary<string, IMemberAssignment>> AssignMembersAsync(string groupId, string protocolType, 
            IImmutableDictionary<string, IMemberMetadata> memberMetadata, 
            IImmutableDictionary<string, IMemberAssignment> currentAssignments,
            CancellationToken cancellationToken)
        {
            var metadata = memberMetadata.First().Value;
            var encoder = _encoders[protocolType];
            var assigner = encoder.GetAssignor(metadata.AssignmentStrategy);

            if (currentAssignments == ImmutableDictionary<string, IMemberAssignment>.Empty) {
                // should only happen when the leader is changed
                var request = new DescribeGroupsRequest(groupId);
                var response = await Router.SendAsync(request, groupId, cancellationToken).ConfigureAwait(false);
                var group = response.Groups.SingleOrDefault(g => g.GroupId == groupId);
                if (group != null) {
                    currentAssignments = group.Members.ToImmutableDictionary(m => m.MemberId, m => m.MemberAssignment);
                }
            }
            return await assigner.AssignMembersAsync(Router, memberMetadata, currentAssignments, cancellationToken).ConfigureAwait(false);
        }

        public async Task<IMemberAssignment> SyncGroupAsync(string groupId, string memberId, int generationId, string protocolType, CancellationToken cancellationToken)
        {
            var request = new SyncGroupRequest(groupId, generationId, memberId);
            var response = await Router.SendAsync(request, groupId, cancellationToken, retryPolicy: Configuration.GroupCoordinationRetry).ConfigureAwait(false);
            if (!response.ErrorCode.IsSuccess()) {
                throw request.ExtractExceptions(response);
            }
            return response.MemberAssignment;
        }
    }
}