using System;
using KafkaClient.Common;

namespace KafkaClient.Connection
{
    public class KafkaConnectionFactory : IKafkaConnectionFactory
    {
        public IKafkaConnection Create(KafkaEndpoint endpoint, TimeSpan? requestTimeout, IKafkaLog log, int maxRetry, TimeSpan? connectingTimeout = null, bool trackTelemetry = false)
        {
            var socket = new KafkaTcpSocket(log, endpoint, maxRetry, connectingTimeout, trackTelemetry);
            return new KafkaConnection(socket, requestTimeout, log);
        }

        public KafkaEndpoint Resolve(Uri kafkaAddress, IKafkaLog log)
        {
            return KafkaEndpoint.Resolve(kafkaAddress, log);
        }
    }
}