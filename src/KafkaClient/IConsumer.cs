﻿using System;
using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Protocol;
using KafkaClient.Protocol.Types;

namespace KafkaClient
{
    public interface IConsumer
    {
        /// <summary>
        /// Explicit fetch for topic/partition. This does not use consumer groups.
        /// </summary>
        Task<IImmutableList<Message>> FetchMessagesAsync(string topicName, int partitionId, long offset, int maxCount, CancellationToken cancellationToken);

        /// <summary>
        /// The configuration for various limits and for consume defaults
        /// </summary>
        IConsumerConfiguration Configuration { get; }

        IImmutableDictionary<string, IProtocolTypeEncoder> Encoders { get; }

        Task<IConsumerGroupMember> JoinConsumerGroupAsync(string groupId, IMemberMetadata metadata, CancellationToken cancellationToken, string memberId = "");
    }

    public interface IConsumerGroupMember : IGroupMember, IDisposable
    {
        string LeaderId { get; }

        int GenerationId { get; }

        // is this necessary?
        IProtocolTypeEncoder Encoder { get; }
    }
}