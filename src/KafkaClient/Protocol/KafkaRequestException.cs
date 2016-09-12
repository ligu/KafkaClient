using System;
using System.Runtime.Serialization;
using KafkaClient.Connection;

namespace KafkaClient.Protocol
{
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
}