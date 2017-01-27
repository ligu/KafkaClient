namespace KafkaClient.Protocol
{
    /// <summary>
    /// An exception caused by a Kafka Request for fetching (FetchRequest, FetchOffset, etc)
    /// </summary>
    public class FetchOutOfRangeException : RequestException
    {
        public FetchOutOfRangeException(string message)
            : base(message)
        {
        }

        public FetchOutOfRangeException(FetchRequest.Topic topic, ApiKey apiKey, ErrorResponseCode errorCode)
            : base(apiKey, errorCode, topic.ToString())
        {
            Topic = topic;
        }

        public FetchRequest.Topic Topic { get; }
    }
}