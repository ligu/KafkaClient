using System;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// An exception caused by a Kafka Request for fetching (FetchRequest, FetchOffset, etc)
    /// </summary>
    public class FetchOutOfRangeException : RequestException
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

        public Fetch Fetch { get; }
    }
}