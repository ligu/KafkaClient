using System;
using System.Runtime.Serialization;
using KafkaClient.Protocol;

namespace KafkaClient
{
    /// <summary>
    /// An exception cause by invalid/missing/out-of-date metadata in the local metadata cache
    /// </summary>
    [Serializable]
    public class CachedMetadataException : KafkaException
    {
        public CachedMetadataException(string message)
            : base(message)
        {
        }

        public CachedMetadataException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public CachedMetadataException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            Topic = info.GetString("Topic");
            var value = info.GetInt32("Partition");
            if (value >= 0) {
                Partition = value;
            }
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue("Topic", Topic);
            info.AddValue("Partition", Partition.GetValueOrDefault(-1));
        }

        public string Topic { get; set; }
        public int? Partition { get; set; }
    }
}