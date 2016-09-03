using System;
using System.Runtime.Serialization;
using KafkaNet.Model;

namespace KafkaNet.Protocol
{
    [Serializable]
    public class CrcValidationException : KafkaException
    {
        public CrcValidationException(uint crc, uint calculatedCrc)
            : base("Calculated CRC did not match reported CRC.")
        {
            Crc = crc;
            CalculatedCrc = calculatedCrc;
        }

        public CrcValidationException(string message)
            : base(message)
        {
        }

        public CrcValidationException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public CrcValidationException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            Crc = info.GetUInt32("Crc");
            CalculatedCrc = info.GetUInt32("CalculatedCrc");
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue("Crc", Crc);
            info.AddValue("CalculatedCrc", CalculatedCrc);
        }

        public uint Crc { get; set; }
        public uint CalculatedCrc { get; set; }
    }

    [Serializable]
    public class BufferUnderRunException : KafkaException
    {
        public BufferUnderRunException(int messageHeaderSize, int requiredBufferSize, long remainingBufferSize)
            : base("The size of the message from Kafka exceeds the provide buffer size.")
        {
            MessageHeaderSize = messageHeaderSize;
            RequiredBufferSize = requiredBufferSize;
            RemainingBufferSize = remainingBufferSize;
        }

        public BufferUnderRunException(string message)
            : base(message)
        {
        }

        public BufferUnderRunException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public BufferUnderRunException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            MessageHeaderSize = info.GetInt32("MessageHeaderSize");
            RequiredBufferSize = info.GetInt32("RequiredBufferSize");
            RemainingBufferSize = info.GetInt64("RemainingBufferSize");
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue("MessageHeaderSize", MessageHeaderSize);
            info.AddValue("RequiredBufferSize", RequiredBufferSize);
            info.AddValue("RemainingBufferSize", RemainingBufferSize);
        }

        public int MessageHeaderSize { get; set; }
        public int RequiredBufferSize { get; set; }
        public long RemainingBufferSize { get; set; }
    }

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

    /// <summary>
    /// An exception cause by a Kafka Request
    /// </summary>
    [Serializable]
    public class KafkaRequestException : KafkaException
    {
        public KafkaRequestException(string message)
            : base(message)
        {
        }

        public KafkaRequestException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public KafkaRequestException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            ApiKey = (ApiKeyRequestType)info.GetInt16("ApiKey");
            Endpoint = info.GetValue<KafkaEndpoint>("Endpoint");
            ErrorCode = (ErrorResponseCode)info.GetInt16("ErrorCode");
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue("ApiKey", (short)ApiKey);
            info.AddValue("Endpoint", Endpoint);
            info.AddValue("ErrorCode", (short)ErrorCode);
        }

        public ApiKeyRequestType ApiKey { get; set; }
        public KafkaEndpoint Endpoint { get; set; }
        public ErrorResponseCode ErrorCode { get; set; }
    }

    /// <summary>
    /// An exception cause by a failure in the connection to Kafka
    /// </summary>
    [Serializable]
    public class KafkaConnectionException : KafkaException
    {
        public KafkaConnectionException(string message)
            : base(message)
        {
        }

        public KafkaConnectionException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public KafkaConnectionException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            Endpoint = info.GetValue<KafkaEndpoint>("Endpoint");
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue("Endpoint", Endpoint);
        }

        public KafkaEndpoint Endpoint { get; set; }
    }

    /// <summary>
    /// Base class for all exceptions related to kafka, to make it easier to handle them en mass
    /// </summary>
    [Serializable]
    public class KafkaException : ApplicationException
    {
        public KafkaException(string message)
            : base(message)
        {
        }

        public KafkaException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public KafkaException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}
