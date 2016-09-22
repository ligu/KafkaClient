using System;
using KafkaClient.Common;

namespace KafkaClient.Connection
{
    public class KafkaConnectionFactory : IKafkaConnectionFactory
    {
        public IKafkaConnection Create(KafkaEndpoint endpoint, IKafkaConnectionConfiguration configuration, IKafkaLog log)
        {
            var socket = new KafkaTcpSocket(log, endpoint, configuration.MaxRetries, configuration.ConnectingTimeout, configuration.TrackTelemetry);
            return new KafkaConnection(socket, configuration.RequestTimeout, log);
        }

        public KafkaEndpoint Resolve(Uri kafkaAddress, IKafkaLog log)
        {
            return KafkaEndpoint.Resolve(kafkaAddress, log);
        }
    }
}