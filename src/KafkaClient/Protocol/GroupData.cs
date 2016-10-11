using System;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    public abstract class GroupData : IEquatable<GroupData>
    {
        private static readonly byte[] Empty = {};

        protected GroupData(string id, byte[] data)
        {
            Id = id;
            Data = data ?? Empty;
        }

        protected string Id { get; }
        protected byte[] Data { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as GroupData);
        }

        /// <inheritdoc />
        public bool Equals(GroupData other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return string.Equals(Id, other.Id) 
                   && Data.HasEqualElementsInOrder(other.Data);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                return ((Id?.GetHashCode() ?? 0)*397) ^ (Data?.GetHashCode() ?? 0);
            }
        }

        /// <inheritdoc />
        public static bool operator ==(GroupData left, GroupData right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(GroupData left, GroupData right)
        {
            return !Equals(left, right);
        }        
    }
}