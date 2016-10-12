using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    public class MetadataTopic : IEquatable<MetadataTopic>
    {
        public MetadataTopic(string topicName, ErrorResponseCode errorCode = ErrorResponseCode.None, IEnumerable<MetadataPartition> partitions = null, bool? isInternal = null)
        {
            ErrorCode = errorCode;
            TopicName = topicName;
            IsInternal = isInternal;
            Partitions = ImmutableList<MetadataPartition>.Empty.AddNotNullRange(partitions);
        }

        public ErrorResponseCode ErrorCode { get; }

        public string TopicName { get; }
        public bool? IsInternal { get; }

        public IImmutableList<MetadataPartition> Partitions { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as MetadataTopic);
        }

        /// <inheritdoc />
        public bool Equals(MetadataTopic other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return ErrorCode == other.ErrorCode 
                && string.Equals(TopicName, other.TopicName) 
                && IsInternal == other.IsInternal
                && Partitions.HasEqualElementsInOrder(other.Partitions);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = (int) ErrorCode;
                hashCode = (hashCode*397) ^ (TopicName?.GetHashCode() ?? 0);
                hashCode = (hashCode*397) ^ (IsInternal?.GetHashCode() ?? 0);
                hashCode = (hashCode*397) ^ (Partitions?.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        /// <inheritdoc />
        public static bool operator ==(MetadataTopic left, MetadataTopic right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(MetadataTopic left, MetadataTopic right)
        {
            return !Equals(left, right);
        }
    }
}