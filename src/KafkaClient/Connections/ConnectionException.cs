using System;
using System.Runtime.Serialization;
using KafkaClient.Common;

namespace KafkaClient.Connections
{
    /// <summary>
    /// An exception cause by a failure in the connection to Kafka
    /// </summary>
    [Serializable]
    public class ConnectionException : KafkaException
    {
        public ConnectionException(Endpoint endpoint)
            : base($"Lost connection to {endpoint}")
        {
            Endpoint = endpoint;
        }

        public ConnectionException(Endpoint endpoint, Exception innerException)
            : base($"Lost connection to {endpoint}", innerException)
        {
            Endpoint = endpoint;
        }

        public ConnectionException(string message)
            : base(message)
        {
        }

        public ConnectionException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public ConnectionException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            Endpoint = info.GetValue<Endpoint>(nameof(Endpoint));
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue(nameof(Endpoint), Endpoint);
        }

        public Endpoint Endpoint { get; set; }
    }
}