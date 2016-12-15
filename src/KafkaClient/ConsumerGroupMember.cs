using System;
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

        public ConsumerGroupMember(IConsumer consumer, JoinGroupRequest request, JoinGroupResponse response, IProtocolTypeEncoder encoder, ILog log = null)
        {
            _consumer = consumer;
            _log = log ?? TraceLog.Log;

            GroupId = request.GroupId;
            MemberId = response.MemberId;
            LeaderId = response.LeaderId;
            GenerationId = response.GenerationId;
            Encoder = encoder;

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

        public string GroupId { get; }
        public string MemberId { get; }

        public string LeaderId { get; }
        public int GenerationId { get; }
        public IProtocolTypeEncoder Encoder { get; }

        private async Task DedicatedHeartbeatTask()
        {
            try {
                // only allow one heartbeat to execute, dump out all other requests
                if (Interlocked.Increment(ref _activeHeartbeatCount) != 1) return;

                while (!_disposeToken.IsCancellationRequested) {
                    await Task.Delay(_heartbeatTimeout, _disposeToken.Token);
                    await _consumer.SendHeartbeatAsync(GroupId, MemberId, GenerationId, _disposeToken.Token);
                }
            } catch (Exception ex) when (!(ex is TaskCanceledException)) {
                _log.Warn(() => LogEvent.Create(ex));
            } finally {
                Interlocked.Decrement(ref _activeHeartbeatCount);
                _log.Info(() => LogEvent.Create($"Closed down heartbeat for {GroupId}/{MemberId}"));
            }
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