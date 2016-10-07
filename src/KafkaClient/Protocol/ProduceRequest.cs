using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    public class ProduceRequest : Request, IRequest<ProduceResponse>, IEquatable<ProduceRequest>
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
            Payloads = ImmutableList<Payload>.Empty.AddNotNullRange(payload);
        }

        /// <summary>
        /// Time kafka will wait for requested ack level before returning.
        /// </summary>
        public TimeSpan Timeout { get; }

        /// <summary>
        /// Level of ack required by kafka: 0 immediate, 1 written to leader, 2+ replicas synced, -1 all replicas
        /// </summary>
        public short Acks { get; }

        /// <summary>
        /// Collection of payloads to post to kafka
        /// </summary>
        public IImmutableList<Payload> Payloads { get; }

        #region Equality 

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as ProduceRequest);
        }

        /// <inheritdoc />
        public bool Equals(ProduceRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Timeout.Equals(other.Timeout) 
                && Acks == other.Acks 
                && Payloads.HasEqualElementsInOrder(other.Payloads);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = Timeout.GetHashCode();
                hashCode = (hashCode*397) ^ Acks.GetHashCode();
                hashCode = (hashCode*397) ^ (Payloads?.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        /// <inheritdoc />
        public static bool operator ==(ProduceRequest left, ProduceRequest right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(ProduceRequest left, ProduceRequest right)
        {
            return !Equals(left, right);
        }

        #endregion
    }
}