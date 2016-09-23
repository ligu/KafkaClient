using System;
using KafkaClient.Common;

namespace KafkaClient.Connection
{
    public class KafkaConnectionFactory : IKafkaConnectionFactory
    {
        public IKafkaConnection Create(KafkaEndpoint endpoint, IKafkaConnectionConfiguration configuration, IKafkaLog log = null)
        {
            return new KafkaConnection(new KafkaTcpSocket(endpoint, configuration, log), configuration, log);
        }

        public KafkaEndpoint Resolve(Uri kafkaAddress, IKafkaLog log)
        {
            return KafkaEndpoint.Resolve(kafkaAddress, log);
        }
    }
}