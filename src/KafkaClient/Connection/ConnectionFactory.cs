using System;
using KafkaClient.Common;

namespace KafkaClient.Connection
{
    public class ConnectionFactory : IConnectionFactory
    {
        public IConnection Create(Endpoint endpoint, IConnectionConfiguration configuration, ILog log = null)
        {
            return new Connection(new TcpSocket(endpoint, configuration, log), configuration, log);
        }

        public Endpoint Resolve(Uri kafkaAddress, ILog log)
        {
            return Endpoint.Resolve(kafkaAddress, log);
        }
    }
}