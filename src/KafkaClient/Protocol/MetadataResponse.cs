using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    public class MetadataResponse : IResponse, IEquatable<MetadataResponse>
    {
        public MetadataResponse(IEnumerable<Broker> brokers = null, IEnumerable<MetadataTopic> topics = null)
        {
            Brokers = ImmutableList<Broker>.Empty.AddNotNullRange(brokers);
            Topics = ImmutableList<MetadataTopic>.Empty.AddNotNullRange(topics);
            Errors = ImmutableList<ErrorResponseCode>.Empty.AddRange(Topics.Select(t => t.ErrorCode));
        }

        public IImmutableList<ErrorResponseCode> Errors { get; }

        public IImmutableList<Broker> Brokers { get; }
        public IImmutableList<MetadataTopic> Topics { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as MetadataResponse);
        }

        /// <inheritdoc />
        public bool Equals(MetadataResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Brokers.HasEqualElementsInOrder(other.Brokers) 
                && Topics.HasEqualElementsInOrder(other.Topics);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                return ((Brokers?.GetHashCode() ?? 0)*397) ^ (Topics?.GetHashCode() ?? 0);
            }
        }

        /// <inheritdoc />
        public static bool operator ==(MetadataResponse left, MetadataResponse right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(MetadataResponse left, MetadataResponse right)
        {
            return !Equals(left, right);
        }
    }
}