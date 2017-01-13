﻿using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Assignment;
using KafkaClient.Protocol;

namespace KafkaClient
{
    public interface IConsumer : IDisposable
    {
        /// <summary>
        /// Explicit fetch for topic/partition. This does not use consumer groups.
        /// </summary>
        Task<IMessageBatch> FetchBatchAsync(string topicName, int partitionId, long offset, CancellationToken cancellationToken, int? batchSize = null);

        /// <summary>
        /// The configuration for various limits and for consume defaults
        /// </summary>
        IConsumerConfiguration Configuration { get; }

        IRouter Router { get; }

        Task<IConsumerMember> JoinGroupAsync(string groupId, string protocolType, IEnumerable<IMemberMetadata> metadata, CancellationToken cancellationToken, IConsumerMember member = null);
        Task<IImmutableDictionary<string, IMemberAssignment>> SyncGroupAsync(string groupId, string memberId, int generationId, string protocolType, IImmutableDictionary<string, IMemberMetadata> memberMetadata, IImmutableDictionary<string, IMemberAssignment> currentAssignments, CancellationToken cancellationToken);
        Task<SyncGroupResponse> SyncGroupAsync(string groupId, string memberId, int generationId, string protocolType, CancellationToken cancellationToken);
        Task<ErrorResponseCode> SendHeartbeatAsync(string groupId, string memberId, int generationId, CancellationToken cancellationToken);
        Task<IMessageBatch> FetchBatchAsync(string groupId, string memberId, int generationId, string topicName, int partitionId, CancellationToken cancellationToken, int? batchSize = null);
        Task LeaveGroupAsync(string groupId, string memberId, CancellationToken cancellationToken, bool awaitResponse = true);
    }
}