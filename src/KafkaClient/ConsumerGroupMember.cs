using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Protocol;
using KafkaClient.Protocol.Types;
using Nito.AsyncEx;

namespace KafkaClient
{
    public class ConsumerGroupMember : IConsumerGroupMember
    {
        private readonly IConsumer _consumer;

        public ConsumerGroupMember(IConsumer consumer, JoinGroupRequest request, JoinGroupResponse response, ILog log = null)
        {
            _consumer = consumer;
            _log = log ?? TraceLog.Log;

            GroupId = request.GroupId;
            MemberId = response.MemberId;
            ProtocolType = request.ProtocolType;

            OnRejoin(response);

            // This thread will heartbeat on the appropriate frequency
            _heartbeatDelay = TimeSpan.FromMilliseconds(request.SessionTimeout.TotalMilliseconds / 2);
            _heartbeatTimeout = request.SessionTimeout;
            _heartbeatTask = Task.Factory.StartNew(DedicatedHeartbeatTask, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }

        private readonly CancellationTokenSource _disposeToken = new CancellationTokenSource();
        private int _disposeCount;
        private int _activeHeartbeatCount = 0;
        private readonly Task _heartbeatTask;
        private readonly TimeSpan _heartbeatDelay;
        private readonly TimeSpan _heartbeatTimeout;
        private readonly ILog _log;
        private ImmutableDictionary<string, IMemberMetadata> _memberMetadata = ImmutableDictionary<string, IMemberMetadata>.Empty;
        private IMemberAssignment _memberAssignment;

        public string GroupId { get; }
        public string MemberId { get; }

        public bool IsLeader { get; private set; }
        public int GenerationId { get; private set; }

        /// <summary>
        /// See https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Client-side+Assignment+Proposal for details
        /// </summary>
        private async Task DedicatedHeartbeatTask()
        {
            try {
                // only allow one heartbeat to execute, dump out all other requests
                if (Interlocked.Increment(ref _activeHeartbeatCount) != 1) return;

                ErrorResponseCode? response = ErrorResponseCode.None;
                var timestamp = DateTimeOffset.UtcNow;
                while (!(_disposeToken.IsCancellationRequested || HeartbeatIsOverdue(timestamp))) {
                    try {
                        switch (response) {
                            // unrecoverable
                            case ErrorResponseCode.UnknownMemberId:
                            case ErrorResponseCode.IllegalGeneration:
                                _disposeToken.Cancel();
                                continue;

                            case ErrorResponseCode.GroupLoadInProgress: // Down state
                            case ErrorResponseCode.GroupCoordinatorNotAvailable: // Initialize state
                            case ErrorResponseCode.None: // Stable state
                                await Task.Delay(_heartbeatDelay, _disposeToken.Token).ConfigureAwait(false);
                                break;

                            case ErrorResponseCode.RebalanceInProgress: // Joining or AwaitSync state
                                await RejoinGroupAsync(_disposeToken.Token).ConfigureAwait(false);
                                response = null; // => AwaitSync state
                                timestamp = DateTimeOffset.UtcNow;
                                continue;

                            case null: // AwaitSync state
                                await SyncGroupAsync(_disposeToken.Token).ConfigureAwait(false);
                                response = ErrorResponseCode.None; // => Stable state
                                timestamp = DateTimeOffset.UtcNow;
                                continue;

                            default:
                                await Task.Delay(1, _disposeToken.Token).ConfigureAwait(false);
                                break;
                        }

                        response = await _consumer.SendHeartbeatAsync(GroupId, MemberId, GenerationId, _disposeToken.Token).ConfigureAwait(false);
                        if (response.Value.IsSuccess()) {
                            timestamp = DateTimeOffset.UtcNow;
                        }
                    } catch (Exception ex) {
                        if (!(ex is TaskCanceledException)) {
                            _log.Warn(() => LogEvent.Create(ex));
                        }
                    }
                }
                await LeaveGroupAsync(CancellationToken.None);
            } catch (Exception ex) {
                if (!(ex is TaskCanceledException)) {
                    _log.Warn(() => LogEvent.Create(ex));
                }
            } finally {
                Interlocked.Decrement(ref _activeHeartbeatCount);
                _log.Info(() => LogEvent.Create($"Stopped heartbeat for {GroupId}/{MemberId}"));
            }
        }

        private bool HeartbeatIsOverdue(DateTimeOffset lastHeartbeat)
        {
            return _heartbeatTimeout < DateTimeOffset.UtcNow - lastHeartbeat;
        }

        private async Task RejoinGroupAsync(CancellationToken cancellationToken)
        {
            // on success, this will call OnRejoin before returning
            await _consumer.JoinConsumerGroupAsync(GroupId, ProtocolType, _memberMetadata.Values, cancellationToken, this);
        }

        public void OnRejoin(JoinGroupResponse response)
        {
            if (response.MemberId != MemberId) throw new ArgumentOutOfRangeException(nameof(response), $"Member is not valid ({MemberId} != {response.MemberId})");

            // TODO: async lock
            IsLeader = response.LeaderId == MemberId;
            GenerationId = response.GenerationId;
            _memberMetadata = IsLeader 
                ? ImmutableDictionary<string, IMemberMetadata>.Empty.AddRange(response.Members.Select(m => new KeyValuePair<string, IMemberMetadata>(m.MemberId, m.Metadata))) 
                : ImmutableDictionary<string, IMemberMetadata>.Empty;
        }

        private async Task SyncGroupAsync(CancellationToken cancellationToken)
        {
            _memberAssignment = await _consumer.SyncGroupAsync(GroupId, MemberId, GenerationId, ProtocolType, IsLeader ? _memberMetadata : ImmutableDictionary<string, IMemberMetadata>.Empty, cancellationToken);
        }

        /// <summary>
        /// Leave the consumer group and stop heartbeats.
        /// </summary>
        public async Task LeaveGroupAsync(CancellationToken cancellationToken)
        {
            //skip multiple calls to dispose
            if (Interlocked.Increment(ref _disposeCount) != 1) return;

            _disposeToken.Cancel();
            if (cancellationToken == CancellationToken.None) {
                await Task.WhenAny(_heartbeatTask, Task.Delay(TimeSpan.FromSeconds(1), cancellationToken));
                await _consumer.LeaveConsumerGroupAsync(GroupId, MemberId, cancellationToken, false);
            } else {
                await _heartbeatTask.WaitAsync(cancellationToken);
                await _consumer.LeaveConsumerGroupAsync(GroupId, MemberId, cancellationToken);
            }
        }

        public void Dispose()
        {
            AsyncContext.Run(() => LeaveGroupAsync(CancellationToken.None));
        }

        public string ProtocolType { get; }
    }
}