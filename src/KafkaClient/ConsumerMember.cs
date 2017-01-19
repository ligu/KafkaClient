using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Assignment;
using KafkaClient.Common;
using KafkaClient.Protocol;
using Nito.AsyncEx;

namespace KafkaClient
{
    public class ConsumerMember : IConsumerMember
    {
        private readonly IConsumer _consumer;
        private IRouter Router => _consumer.Router;
        private IConsumerConfiguration Configuration => _consumer.Configuration;

        public ConsumerMember(IConsumer consumer, JoinGroupRequest request, JoinGroupResponse response, ILog log = null)
        {
            _consumer = consumer;
            Log = log ?? consumer.Router?.Log ?? TraceLog.Log;

            GroupId = request.GroupId;
            MemberId = response.MemberId;
            ProtocolType = request.ProtocolType;

            OnJoinGroup(response);

            // This thread will heartbeat on the appropriate frequency
            _heartbeatDelay = TimeSpan.FromMilliseconds(request.SessionTimeout.TotalMilliseconds / 2);
            _heartbeatTimeout = request.SessionTimeout;
            _heartbeatTask = Task.Factory.StartNew(DedicatedHeartbeatTask, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }

        private readonly CancellationTokenSource _disposeToken = new CancellationTokenSource();
        private int _disposeCount;
        private int _activeHeartbeatCount;
        private readonly Task _heartbeatTask;
        private readonly TimeSpan _heartbeatDelay;
        private readonly TimeSpan _heartbeatTimeout;

        private readonly AsyncLock _lock = new AsyncLock();
        private readonly AsyncManualResetEvent _isSynced = new AsyncManualResetEvent(false);
        private ImmutableDictionary<string, IMemberMetadata> _memberMetadata = ImmutableDictionary<string, IMemberMetadata>.Empty;
        private IImmutableDictionary<TopicPartition, IMessageBatch> _batches = ImmutableDictionary<TopicPartition, IMessageBatch>.Empty;
        private IMemberAssignment _assignment;

        public ILog Log { get; }
        public bool IsLeader { get; private set; }
        public int GenerationId { get; private set; }
        private string _groupProtocol;

        public string GroupId { get; }
        public string MemberId { get; }

        /// <summary>
        /// State machine for Member state
        /// See https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Client-side+Assignment+Proposal for basis
        /// </summary>
        /// <remarks>
        ///                            +===========+
        ///                            [           ]
        ///     +----------------------+  Assign   ]
        ///     |                      [           ]
        ///     |                      +=====+=====+
        ///     |                            ^
        ///     | SyncGroupRequest           | JoinGroupResponse
        ///     | (only leader assigns)      | ErrorResponseCode.None
        ///     |                            |
        ///     |                      +-----+-----+
        ///     |                      |           |
        ///     |                  +--->  Joining  |
        ///     |                  |   |           |
        ///     |                  |   +-----+-----+
        ///     |                  |         |
        ///     |        JoinGroup |         | JoinGroupResponse
        ///     |        Request   |         | ErrorResponseCode.GroupCoordinatorNotAvailable
        ///     |                  |         | ErrorResponseCode.GroupLoadInProgress
        ///     |                  |         v                 
        ///     |                  |   +-----+-----+
        ///     |                  +---+           |
        ///     |                      |  Rejoin   |
        ///     |  +------------------->           |
        ///     |  | SyncGroupResponse +-----+-----+
        ///     v  | RebalanceInProgress     ^
        ///  +--+--+-----+                   | HeartbeatResponse
        ///  |           |                   | ErrorResponseCode.RebalanceInProgress
        ///  |  Syncing  |                   |
        ///  |           |             +-----+--------+
        ///  +-----+-----+             |              |
        ///        |               +---> Heartbeating |
        ///        |               |   |              |
        ///        |               |   +-----+--------+
        ///        |               |         |
        ///        |     Heartbeat |         | HeartbeatResponse
        ///        |     Request   |         | ErrorResponseCode.None
        ///        |               |         v
        ///        |               |   +-----+-----+
        ///        |               +---+           |
        ///        |                   |  Stable   |
        ///        +------------------->           |
        ///        SyncGroupResponse   +-----------+ 
        ///        ErrorResponseCode.None                   
        /// </remarks>
        private enum MemberState
        {
            Assign,
            //Syncing,
            Stable,
            //Heartbeating,
            Rejoin
            //Joining
        }

        /// <summary>
        /// See https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Client-side+Assignment+Proposal for details
        /// </summary>
        private async Task DedicatedHeartbeatTask()
        {
            try {
                // only allow one heartbeat to execute, dump out all other requests
                if (Interlocked.Increment(ref _activeHeartbeatCount) != 1) return;

                var state = MemberState.Assign;
                var response = ErrorResponseCode.None;
                var lastHeartbeat = DateTimeOffset.UtcNow;
                while (!(_disposeToken.IsCancellationRequested || IsOverdue(lastHeartbeat) || IsUnrecoverable(response))) {
                    try {
                        var currentState = state;
                        Log.Info(() => LogEvent.Create($"Local state {currentState} for member {MemberId}"));
                        switch (currentState) {
                            case MemberState.Assign:
                                response = await SyncGroupAsync(_disposeToken.Token).ConfigureAwait(false);
                                if (response.IsSuccess()) {
                                    state = MemberState.Stable;
                                } else if (response == ErrorResponseCode.RebalanceInProgress) {
                                    state = MemberState.Rejoin;
                                }
                                break;

                            case MemberState.Stable:
                                await Task.Delay(_heartbeatDelay, _disposeToken.Token).ConfigureAwait(false);
                                response = await SendHeartbeatAsync(_disposeToken.Token).ConfigureAwait(false);
                                if (response == ErrorResponseCode.RebalanceInProgress) {
                                    state = MemberState.Rejoin;
                                }
                                break;

                            case MemberState.Rejoin:
                                response = await JoinGroupAsync(_disposeToken.Token).ConfigureAwait(false);
                                if (response.IsSuccess()) {
                                    state = MemberState.Assign;
                                } else if (response == ErrorResponseCode.GroupCoordinatorNotAvailable || response == ErrorResponseCode.GroupLoadInProgress) {
                                    state = MemberState.Rejoin;
                                }
                                break;

                            default:
                                Log.Warn(() => LogEvent.Create($"Unexpected local state {currentState} for member {MemberId}"));
                                state = MemberState.Assign;
                                await Task.Delay(1, _disposeToken.Token).ConfigureAwait(false);
                                continue; // while
                        }
                        if (response.IsSuccess()) {
                            lastHeartbeat = DateTimeOffset.UtcNow;
                        }
                    } catch (OperationCanceledException) { // cancellation token fired while attempting to get tasks: normal behavior
                    } catch (Exception ex) {
                        Log.Warn(() => LogEvent.Create(ex));
                        await Task.Delay(1, _disposeToken.Token).ConfigureAwait(false); // to avoid killing the CPU
                    }
                }
                await DisposeAsync(CancellationToken.None).ConfigureAwait(false);
            } catch (OperationCanceledException) { // cancellation token fired while attempting to get tasks: normal behavior
            } catch (Exception ex) {
                Log.Warn(() => LogEvent.Create(ex));
            } finally {
                Interlocked.Decrement(ref _activeHeartbeatCount);
                Log.Info(() => LogEvent.Create($"Stopped heartbeat for {GroupId}/{MemberId}"));
            }
        }

        private bool IsOverdue(DateTimeOffset lastHeartbeat)
        {
            return _heartbeatTimeout < DateTimeOffset.UtcNow - lastHeartbeat;
        }

        private bool IsUnrecoverable(ErrorResponseCode? response)
        {
            return response == ErrorResponseCode.IllegalGeneration 
                || response == ErrorResponseCode.UnknownMemberId;
        }

        private async Task<ErrorResponseCode> JoinGroupAsync(CancellationToken cancellationToken)
        {
            if (_disposeCount > 0) throw new ObjectDisposedException($"Member {MemberId} is no longer valid");

            IEnumerable<JoinGroupRequest.GroupProtocol> protocols = null;
            using (await _lock.LockAsync(cancellationToken).ConfigureAwait(false)) {
                cancellationToken.ThrowIfCancellationRequested();
                if (IsLeader) {
                    protocols = _memberMetadata?.Values.Select(m => new JoinGroupRequest.GroupProtocol(m));
                }
            }

            var request = new JoinGroupRequest(GroupId, Configuration.GroupHeartbeat, MemberId, ProtocolType, protocols, Configuration.GroupRebalanceTimeout);
            var response = await Router.SendAsync(request, GroupId, cancellationToken, retryPolicy: Configuration.GroupCoordinationRetry, context: new RequestContext(protocolType: ProtocolType)).ConfigureAwait(false);
            if (response.ErrorCode.IsSuccess()) {
                OnJoinGroup(response);
            }
            return response.ErrorCode;
        }

        public void OnJoinGroup(JoinGroupResponse response)
        {
            if (response.MemberId != MemberId) throw new ArgumentOutOfRangeException(nameof(response), $"Member is not valid ({MemberId} != {response.MemberId})");
            if (_disposeCount > 0) throw new ObjectDisposedException($"Member {MemberId} is no longer valid");

            using (_lock.Lock()) {
                IsLeader = response.LeaderId == MemberId;
                GenerationId = response.GenerationId;
                _groupProtocol = response.GroupProtocol;
                _memberMetadata = response.Members.ToImmutableDictionary(member => member.MemberId, member => member.Metadata);
            }
        }

        public async Task<ErrorResponseCode> SendHeartbeatAsync(CancellationToken cancellationToken)
        {
            var request = new HeartbeatRequest(GroupId, GenerationId, MemberId);
            var response = await Router.SendAsync(request, GroupId, cancellationToken, retryPolicy: Configuration.GroupCoordinationRetry).ConfigureAwait(false);
            return response.ErrorCode;
        }

        public async Task<ErrorResponseCode> SyncGroupAsync(CancellationToken cancellationToken)
        {
            if (_disposeCount > 0) throw new ObjectDisposedException($"Member {MemberId} is no longer valid");

            IEnumerable<SyncGroupRequest.GroupAssignment> groupAssignments = null;
            using (await _lock.LockAsync(cancellationToken).ConfigureAwait(false)) {
                cancellationToken.ThrowIfCancellationRequested();
                if (IsLeader) {
                    var encoder = _consumer.Encoders[ProtocolType];
                    var assigner = encoder.GetAssignor(_groupProtocol);
                    var assignments = await assigner.AssignMembersAsync(Router, GroupId, GenerationId, _memberMetadata, cancellationToken).ConfigureAwait(false);
                    groupAssignments = assignments.Select(pair => new SyncGroupRequest.GroupAssignment(pair.Key, pair.Value));
                }
            }
            await Task.WhenAll(_batches.Values.Select(b => b.CommitMarkedIgnoringDisposedAsync(cancellationToken))).ConfigureAwait(false);

            var request = new SyncGroupRequest(GroupId, GenerationId, MemberId, groupAssignments);
            var response = await Router.SyncGroupAsync(request, new RequestContext(protocolType: ProtocolType), Configuration.GroupCoordinationRetry, cancellationToken).ConfigureAwait(false);
            if (!response.ErrorCode.IsSuccess()) return response.ErrorCode;

            using (await _lock.LockAsync(cancellationToken).ConfigureAwait(false)) {
                cancellationToken.ThrowIfCancellationRequested();
                _assignment = response.MemberAssignment;
                var validPartitions = _assignment.PartitionAssignments.ToImmutableHashSet();
                var invalidPartitions = _batches.Where(pair => !validPartitions.Contains(pair.Key)).ToList();
                foreach (var invalidPartition in invalidPartitions) {
                    invalidPartition.Value.Dispose();
                }
                _batches = _batches.RemoveRange(invalidPartitions.Select(pair => pair.Key));
                _isSynced.Set();
                return ErrorResponseCode.None;
            }
        }

        /// <summary>
        /// Leave the consumer group and stop heartbeats.
        /// </summary>
        public async Task DisposeAsync(CancellationToken cancellationToken)
        {
            // skip multiple calls to dispose
            if (Interlocked.Increment(ref _disposeCount) != 1) return;

            _isSynced.Set(); // in case anyone is waiting
            _disposeToken.Cancel();
            using (await _lock.LockAsync(cancellationToken).ConfigureAwait(false)) {
                await Task.WhenAll(_batches.Values.Select(b => b.CommitMarkedAsync(cancellationToken))).ConfigureAwait(false);
                foreach (var batch in _batches.Values) {
                    batch.Dispose();
                }
                _batches = ImmutableDictionary<TopicPartition, IMessageBatch>.Empty;
                _assignment = null;

                if (cancellationToken == CancellationToken.None) {
                    await Task.WhenAny(_heartbeatTask, Task.Delay(TimeSpan.FromSeconds(1), cancellationToken)).ConfigureAwait(false);
                    var request = new LeaveGroupRequest(GroupId, MemberId, false);
                    await Router.SendAsync(request, GroupId, cancellationToken, retryPolicy: Configuration.GroupCoordinationRetry).ConfigureAwait(false);
                } else {
                    await _heartbeatTask.WaitAsync(cancellationToken);
                    var request = new LeaveGroupRequest(GroupId, MemberId);
                    var response = await Router.SendAsync(request, GroupId, cancellationToken, retryPolicy: Configuration.GroupCoordinationRetry).ConfigureAwait(false);
                    if (!response.ErrorCode.IsSuccess()) {
                        throw request.ExtractExceptions(response);
                    }
                }
            }
        }

        public void Dispose()
        {
            AsyncContext.Run(() => DisposeAsync(CancellationToken.None));
        }

        public string ProtocolType { get; }

        public async Task<IMessageBatch> FetchBatchAsync(CancellationToken cancellationToken, int? batchSize = null)
        {
            await _isSynced.WaitAsync(cancellationToken).ConfigureAwait(false);
            if (_disposeCount > 0) throw new ObjectDisposedException($"Member {MemberId} is no longer valid");

            int generationId;
            TopicPartition partition;
            using (await _lock.LockAsync(cancellationToken).ConfigureAwait(false)) {
                cancellationToken.ThrowIfCancellationRequested();

                generationId = GenerationId;
                partition = _assignment?.PartitionAssignments.FirstOrDefault(p => !_batches.ContainsKey(p));
            }

            if (partition == null) return MessageBatch.Empty;
            var batch = await _consumer.FetchBatchAsync(GroupId, MemberId, generationId, partition.TopicName, partition.PartitionId, cancellationToken, batchSize).ConfigureAwait(false);

            using (await _lock.LockAsync(cancellationToken).ConfigureAwait(false)) {
                _batches = _batches.Add(partition, batch);
            }
            return batch;
        }
    }
}