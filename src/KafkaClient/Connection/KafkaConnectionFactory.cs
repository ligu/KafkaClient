using System;
using KafkaClient.Common;

namespace KafkaClient.Connection
{
    public class KafkaConnectionFactory : IKafkaConnectionFactory
    {
        public IKafkaConnection Create(KafkaEndpoint endpoint, IKafkaConnectionOptions options, IKafkaLog log)
        {
            var socket = new KafkaTcpSocket(log, endpoint, options.MaxRetries, options.ConnectingTimeout, options.TrackTelemetry);
            return new KafkaConnection(socket, options.RequestTimeout, log);
        }

        public KafkaEndpoint Resolve(Uri kafkaAddress, IKafkaLog log)
        {
            return KafkaEndpoint.Resolve(kafkaAddress, log);
        }
    }
}