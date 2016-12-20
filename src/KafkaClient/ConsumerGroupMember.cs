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

            OnRejoin(response);

            // This thread will heartbeat on the appropriate frequency
            _heartbeatTimeout = TimeSpan.FromMilliseconds(request.SessionTimeout.TotalMilliseconds / 2);
            _heartbeatTask = Task.Factory.StartNew(DedicatedHeartbeatTask, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }

        private readonly CancellationTokenSource _disposeToken = new CancellationTokenSource();
        private int _disposeCount;
        private int _activeHeartbeatCount = 0;
        private readonly Task _heartbeatTask;
        private readonly TimeSpan _heartbeatTimeout;
        private readonly ILog _log;
        private int _joinGeneration;
        private ImmutableDictionary<string, IMemberMetadata> _memberMetadata = ImmutableDictionary<string, IMemberMetadata>.Empty;

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

                var response = ErrorResponseCode.None;
                var timestamp = DateTimeOffset.UtcNow;
                while (!(_disposeToken.IsCancellationRequested || HeartbeatIsOverdue(timestamp, response))) {
                    try {
                        switch (response) {
                            case ErrorResponseCode.GroupLoadInProgress: // Down state
                            case ErrorResponseCode.GroupCoordinatorNotAvailable: // Initialize state
                            case ErrorResponseCode.None: // Stable state
                                await Task.Delay(_heartbeatTimeout, _disposeToken.Token);
                                break;

                            case ErrorResponseCode.RebalanceInProgress: // Joining or AwaitSync state
                                // should only need to submit Joining once per generation
                                // TODO: how to distinguish between Joining and AwaitSync. Also how to send this enough but not too much ...
                                await RejoinGroupAsync(_disposeToken.Token);
                                await SyncGroupAsync(_disposeToken.Token);
                                break;
                        }
                        response = await _consumer.SendHeartbeatAsync(GroupId, MemberId, GenerationId, _disposeToken.Token);
                        if (response.IsSuccess()) {
                            timestamp = DateTimeOffset.UtcNow;
                        }
                    } catch (Exception ex) when (!(ex is TaskCanceledException)) {
                        _log.Warn(() => LogEvent.Create(ex));
                    }
                }
                _disposeToken.Cancel();
            } catch (Exception ex) {
                if (!(ex is TaskCanceledException)) {
                    _log.Warn(() => LogEvent.Create(ex));
                }
            } finally {
                Interlocked.Decrement(ref _activeHeartbeatCount);
                _log.Info(() => LogEvent.Create($"Stopped heartbeat for {GroupId}/{MemberId}"));
            }
        }

        private bool HeartbeatIsOverdue(DateTimeOffset lastHeartbeat, ErrorResponseCode response)
        {
            return response == ErrorResponseCode.IllegalGeneration 
                || response == ErrorResponseCode.UnknownMemberId
                || _heartbeatTimeout < DateTimeOffset.UtcNow - lastHeartbeat;
        }

        private async Task RejoinGroupAsync(CancellationToken cancellationToken)
        {
            if (_joinGeneration == GenerationId) {
                // on success, this will call OnRejoin before returning
                await _consumer.JoinConsumerGroupAsync(GroupId, _memberMetadata.Values, cancellationToken, this);
            }
        }

        public void OnRejoin(JoinGroupResponse response)
        {
            if (response.MemberId != MemberId) throw new ArgumentOutOfRangeException(nameof(response), $"Member is not valid ({MemberId} != {response.MemberId})");

            // TODO: async lock
            IsLeader = response.LeaderId == MemberId;
            GenerationId = response.GenerationId;
            _joinGeneration = response.GenerationId;
            _memberMetadata = ImmutableDictionary<string, IMemberMetadata>.Empty.AddRange(response.Members.Select(m => new KeyValuePair<string, IMemberMetadata>(m.MemberId, m.Metadata)));
        }

        private async Task SyncGroupAsync(CancellationToken cancellationToken)
        {
            
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
    }
}