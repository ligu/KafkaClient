using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    public class ProduceRequest : Request, IRequest<ProduceResponse>
    {
        public ProduceRequest(Payload payload, TimeSpan? timeout = null, short acks = 1)
            : this(new [] { payload }, timeout, acks)
        {
        }

        public ProduceRequest(IEnumerable<Payload> payload, TimeSpan? timeout = null, short acks = 1) 
            : base(ApiKeyRequestType.Produce, acks != 0)
        {
            Timeout = timeout.GetValueOrDefault(TimeSpan.FromSeconds(1));
            Acks = acks;
            Payload = ImmutableList<Payload>.Empty.AddNotNullRange(payload);
        }

        /// <summary>
        /// Time kafka will wait for requested ack level before returning.
        /// </summary>
        public TimeSpan Timeout { get; }

        /// <summary>
        /// Level of ack required by kafka.  0 immediate, 1 written to leader, 2+ replicas synced, -1 all replicas
        /// </summary>
        public short Acks { get; }

        /// <summary>
        /// Collection of payloads to post to kafka
        /// </summary>
        public ImmutableList<Payload> Payload { get; }
    }
}