using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using KafkaNet.Model;

namespace KafkaNet.Protocol
{
    public static class Extensions
    {
        public static Exception ExtractExceptions<TRequest, TResponse>(this TRequest request, TResponse response, KafkaEndpoint endpoint = null) 
            where TRequest : IKafkaRequest<TResponse> 
            where TResponse : IKafkaResponse
        {
            var exceptions = new List<Exception>();
            foreach (var errorCode in response.Errors.Where(e => e != ErrorResponseCode.NoError)) {
                exceptions.Add(ExtractException(request, errorCode, endpoint));
            }
            if (exceptions.Count == 0) return new KafkaRequestException(request.ApiKey, ErrorResponseCode.NoError) { Endpoint = endpoint };
            if (exceptions.Count == 1) return exceptions[0];
            return new AggregateException(exceptions);
        }

        public static Exception ExtractException(this IKafkaRequest request, ErrorResponseCode errorCode, KafkaEndpoint endpoint) 
        {
            var exception = ExtractFetchException(request as FetchRequest, errorCode) ??
                            new KafkaRequestException(request.ApiKey, errorCode);
            exception.Endpoint = endpoint;
            return exception;
        }

        private static FetchOutOfRangeException ExtractFetchException(FetchRequest request, ErrorResponseCode errorCode)
        {
            if (errorCode == ErrorResponseCode.OffsetOutOfRange && request?.Fetches?.Count == 1) {
                var fetch = request.Fetches.First();
                return new FetchOutOfRangeException(fetch, request.ApiKey, errorCode);
            }
            return null;
        } 

        private static readonly DateTime UnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public static long ToUnixEpochMilliseconds(this DateTime pointInTime)
        {
            return pointInTime > UnixEpoch ? (long)(pointInTime - UnixEpoch).TotalMilliseconds : 0L;
        }

        public static DateTime FromUnixEpochMilliseconds(this long milliseconds)
        {
            return UnixEpoch.AddMilliseconds(milliseconds);
        }
    }

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
                Fetch = new Fetch {
                    MaxBytes = info.GetInt32(nameof(Fetch.MaxBytes)),
                    Offset = info.GetInt64(nameof(Fetch.Offset)),
                    PartitionId = info.GetInt32(nameof(Fetch.PartitionId)),
                    Topic = info.GetString(nameof(Fetch.Topic))
                };
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
                info.AddValue(nameof(Fetch.Topic), Fetch.Topic);
            }         
        }

        public Fetch Fetch { get; }
    }

    /// <summary>
    /// An exception caused by a Kafka Request
    /// </summary>
    [Serializable]
    public class KafkaRequestException : KafkaException
    {
        public KafkaRequestException(ApiKeyRequestType apiKey, ErrorResponseCode errorCode, string message = null)
            : base(message ?? $"Kafka returned error response for {apiKey}: {errorCode}")
        {
            ApiKey = apiKey;
            ErrorCode = errorCode;
        }

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
            ApiKey = (ApiKeyRequestType)info.GetInt16(nameof(ApiKey));
            ErrorCode = (ErrorResponseCode)info.GetInt16(nameof(ErrorCode));
            Endpoint = info.GetValue<KafkaEndpoint>(nameof(Endpoint));
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue(nameof(ApiKey), (short)ApiKey);
            info.AddValue(nameof(ErrorCode), (short)ErrorCode);
            info.AddValue(nameof(Endpoint), Endpoint);
        }

        public ApiKeyRequestType ApiKey { get; }
        public ErrorResponseCode ErrorCode { get; }
        public KafkaEndpoint Endpoint { get; set; }
    }

    /// <summary>
    /// An exception cause by a failure in the connection to Kafka
    /// </summary>
    [Serializable]
    public class KafkaConnectionException : KafkaException
    {
        public KafkaConnectionException(KafkaEndpoint endpoint)
            : base($"Lost connection to server: {endpoint}")
        {
            Endpoint = endpoint;
        }

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
            Endpoint = info.GetValue<KafkaEndpoint>(nameof(Endpoint));
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue(nameof(Endpoint), Endpoint);
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
