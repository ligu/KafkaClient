namespace KafkaNet.Protocol
{
    public class OffsetCommit
    {
        /// <summary>
        /// The topic the offset came from.
        /// </summary>
        public string Topic { get; set; }

        /// <summary>
        /// The partition the offset came from.
        /// </summary>
        public int PartitionId { get; set; }

        /// <summary>
        /// The offset number to commit as completed.
        /// </summary>
        public long Offset { get; set; }

        /// <summary>
        /// If the time stamp field is set to -1, then the broker sets the time stamp to the receive time before committing the offset.
        /// Only version 1 (0.8.2)
        /// </summary>
        public long TimeStamp { get; set; } = -1;

        /// <summary>
        /// Descriptive metadata about this commit.
        /// </summary>
        public string Metadata { get; set; }
    }
}