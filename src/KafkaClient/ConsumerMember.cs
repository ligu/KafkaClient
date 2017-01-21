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
        private Task _disposal;
        private int _disposeCount;
        private int _syncCount;

        private int _activeHeartbeatCount;
        private readonly Task _heartbeatTask;
        private readonly TimeSpan _heartbeatDelay;
        private readonly TimeSpan _heartbeatTimeout;

        private readonly SemaphoreSlim _joinSemaphore = new SemaphoreSlim(1, 1);
        private readonly SemaphoreSlim _syncSemaphore = new SemaphoreSlim(1, 1);
        private readonly SemaphoreSlim _fetchSemaphore = new SemaphoreSlim(0, 1);
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
                Dispose();
                await _disposal.ConfigureAwait(false);
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

            var protocols = _joinSemaphore.Lock(() => IsLeader ? _memberMetadata?.Values.Select(m => new JoinGroupRequest.GroupProtocol(m)) : null, cancellationToken);

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

            _joinSemaphore.Lock(
                () => {
                    IsLeader = response.LeaderId == MemberId;
                    GenerationId = response.GenerationId;
                    _groupProtocol = response.GroupProtocol;
                    _memberMetadata = response.Members.ToImmutableDictionary(member => member.MemberId, member => member.Metadata);
                }, _disposeToken.Token);
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

            var groupAssignments = await _joinSemaphore.LockAsync(
                async () => {
                    if (IsLeader) {
                        var encoder = _consumer.Encoders[ProtocolType];
                        var assigner = encoder.GetAssignor(_groupProtocol);
                        var assignments = await assigner.AssignMembersAsync(Router, GroupId, GenerationId, _memberMetadata, cancellationToken).ConfigureAwait(false);
                        return assignments.Select(pair => new SyncGroupRequest.GroupAssignment(pair.Key, pair.Value));
                    }
                    return null;
                }, _disposeToken.Token).ConfigureAwait(false);
            await Task.WhenAll(_batches.Values.Select(b => b.CommitMarkedIgnoringDisposedAsync(cancellationToken))).ConfigureAwait(false);

            var request = new SyncGroupRequest(GroupId, GenerationId, MemberId, groupAssignments);
            var response = await Router.SyncGroupAsync(request, new RequestContext(protocolType: ProtocolType), Configuration.GroupCoordinationRetry, cancellationToken).ConfigureAwait(false);
            if (!response.ErrorCode.IsSuccess()) return response.ErrorCode;

            _syncSemaphore.Lock(
                () => {
                    _assignment = response.MemberAssignment;
                    var validPartitions = _assignment.PartitionAssignments.ToImmutableHashSet();
                    var invalidPartitions = _batches.Where(pair => !validPartitions.Contains(pair.Key)).ToList();
                    foreach (var invalidPartition in invalidPartitions) {
                        invalidPartition.Value.Dispose();
                    }
                    _batches = _batches.RemoveRange(invalidPartitions.Select(pair => pair.Key));
                }, _disposeToken.Token);
            if (Interlocked.Increment(ref _syncCount) == 1) {
                _fetchSemaphore.Release();
            }
            return ErrorResponseCode.None;
        }

        private async Task DisposeAsync()
        {
            var batches = Interlocked.Exchange(ref _batches, ImmutableDictionary<TopicPartition, IMessageBatch>.Empty);
            await Task.WhenAll(batches.Values.Select(b => b.CommitMarkedAsync(CancellationToken.None))).ConfigureAwait(false);
            foreach (var batch in batches.Values) {
                batch.Dispose();
            }
            _assignment = null;

            await Task.WhenAny(_heartbeatTask, Task.Delay(TimeSpan.FromSeconds(1), CancellationToken.None)).ConfigureAwait(false);
            var request = new LeaveGroupRequest(GroupId, MemberId, false);
            await Router.SendAsync(request, GroupId, CancellationToken.None, retryPolicy: Configuration.GroupCoordinationRetry).ConfigureAwait(false);

            _disposeToken.Dispose();
        }

        /// <summary>
        /// Leave the consumer group and stop heartbeats.
        /// </summary>
        public void Dispose()
        {
            // skip multiple calls to dispose
            if (Interlocked.Increment(ref _disposeCount) != 1) return;

            _disposeToken.Cancel();
            _fetchSemaphore.Dispose();
            _joinSemaphore.Dispose();
            _syncSemaphore.Dispose();
            _disposal = DisposeAsync();
        }

        public string ProtocolType { get; }

        public async Task<IMessageBatch> FetchBatchAsync(CancellationToken cancellationToken, int? batchSize = null)
        {
            return await _fetchSemaphore.LockAsync(
                async () => {
                    if (_disposeCount > 0) throw new ObjectDisposedException($"Member {MemberId} is no longer valid");

                    var generationId = GenerationId;
                    var partition = _syncSemaphore.Lock(() => _assignment?.PartitionAssignments.FirstOrDefault(p => !_batches.ContainsKey(p)), _disposeToken.Token);

                    if (partition == null) return MessageBatch.Empty;
                    var batch = await _consumer.FetchBatchAsync(GroupId, MemberId, generationId, partition.TopicName, partition.PartitionId, cancellationToken, batchSize).ConfigureAwait(false);            
                    _syncSemaphore.Lock(() => _batches = _batches.Add(partition, batch), cancellationToken);
                    return batch;                    
                }, cancellationToken).ConfigureAwait(false);
        }
    }
}