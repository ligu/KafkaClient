using System;
using System.Runtime.Serialization;

namespace KafkaClient.Protocol
{
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