using System;

namespace KafkaNet.Protocol
{
    public class OffsetFetchTopic : TopicResponse, IEquatable<OffsetFetchTopic>
    {
        public OffsetFetchTopic(string topic, int partitionId, ErrorResponseCode errorCode, long offset, string metadata) 
            : base(topic, partitionId, errorCode)
        {
            Offset = offset;
            MetaData = metadata;
        }

        /// <summary>
        /// The offset position saved to the server.
        /// </summary>
        public long Offset { get; }

        /// <summary>
        /// Any arbitrary metadata stored during a CommitRequest.
        /// </summary>
        public string MetaData { get; }

        #region Equality

        public override bool Equals(object obj)
        {
            return Equals(obj as OffsetFetchTopic);
        }

        public bool Equals(OffsetFetchTopic other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                   && Offset == other.Offset 
                   && string.Equals(MetaData, other.MetaData);
        }

        public override int GetHashCode()
        {
            unchecked {
                int hashCode = base.GetHashCode();
                hashCode = (hashCode*397) ^ Offset.GetHashCode();
                hashCode = (hashCode*397) ^ (MetaData?.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        public static bool operator ==(OffsetFetchTopic left, OffsetFetchTopic right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(OffsetFetchTopic left, OffsetFetchTopic right)
        {
            return !Equals(left, right);
        }        

        #endregion

        public override string ToString()
        {
            return $"[OffsetFetchResponse TopicName={TopicName}, PartitionID={PartitionId}, Offset={Offset}, MetaData={MetaData}, ErrorCode={ErrorCode}]";
        }
    }
}