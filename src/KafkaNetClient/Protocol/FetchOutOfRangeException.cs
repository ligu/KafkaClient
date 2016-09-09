using System;
using System.Runtime.Serialization;

namespace KafkaNet.Protocol
{
    /// <summary>
    /// An exception caused by a Kafka Request for fetching (FetchRequest, FetchOffset, etc)
    /// </summary>
    [Serializable]
    public class FetchOutOfRangeException : KafkaRequestException
    {
        public FetchOutOfRangeException(Fetch fetch, ApiKeyRequestType apiKey, ErrorResponseCode errorCode, string message = null)
            : base(apiKey, errorCode, message)
        {
            Fetch = fetch;
        }

        public FetchOutOfRangeException(string message)
            : base(message)
        {
        }

        public FetchOutOfRangeException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public FetchOutOfRangeException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            if (info.GetByte(nameof(Fetch)) == 1) {
                var maxBytes = info.GetInt32(nameof(Fetch.MaxBytes));
                var offset = info.GetInt64(nameof(Fetch.Offset));
                var partitionId = info.GetInt32(nameof(Fetch.PartitionId));
                var topicName = info.GetString(nameof(Fetch.TopicName));
                Fetch = new Fetch(topicName, partitionId, offset, maxBytes);
            }
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            if (Fetch == null) {
                info.AddValue(nameof(Fetch), (byte)0);
            } else {
                info.AddValue(nameof(Fetch), (byte)1);
                info.AddValue(nameof(Fetch.MaxBytes), Fetch.MaxBytes);
                info.AddValue(nameof(Fetch.Offset), Fetch.Offset);
                info.AddValue(nameof(Fetch.PartitionId), Fetch.PartitionId);
                info.AddValue(nameof(Fetch.TopicName), Fetch.TopicName);
            }         
        }

        public Fetch Fetch { get; }
    }
}