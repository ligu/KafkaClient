namespace KafkaNet.Protocol
{
    public class Offset
    {
        public Offset()
        {
            Time = -1;
            MaxOffsets = 1;
        }

        public string Topic { get; set; }
        public int PartitionId { get; set; }

        /// <summary>
        /// Used to ask for all messages before a certain time (ms). There are two special values.
        /// Specify -1 to receive the latest offsets and -2 to receive the earliest available offset.
        /// Note that because offsets are pulled in descending order, asking for the earliest offset will always return you a single element.
        /// </summary>
        public long Time { get; set; }

        public int MaxOffsets { get; set; }
    }
}