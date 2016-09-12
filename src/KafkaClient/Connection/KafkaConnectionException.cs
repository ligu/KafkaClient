using System;
using System.Runtime.Serialization;
using KafkaClient.Protocol;

namespace KafkaClient.Connection
{
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
}