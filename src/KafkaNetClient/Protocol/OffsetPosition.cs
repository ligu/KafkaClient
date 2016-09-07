namespace KafkaNet.Protocol
{
    public class OffsetPosition
    {
        public OffsetPosition()
        {
        }

        public OffsetPosition(int partitionId, long offset)
        {
            PartitionId = partitionId;
            Offset = offset;
        }

        public int PartitionId { get; set; }
        public long Offset { get; set; }

        public override string ToString()
        {
            return $"PartitionId:{PartitionId}, Offset:{Offset}";
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((OffsetPosition)obj);
        }

        protected bool Equals(OffsetPosition other)
        {
            return PartitionId == other.PartitionId && Offset == other.Offset;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (PartitionId * 397) ^ Offset.GetHashCode();
            }
        }
    }
}