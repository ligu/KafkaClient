using System;
using System.Threading.Tasks;
using KafkaClient.Common;

namespace KafkaClient.Connections
{
    public class ConnectionFactory : IConnectionFactory
    {
        /// <inheritdoc />
        public IConnection Create(Endpoint endpoint, IConnectionConfiguration configuration, ILog log = null)
        {
            return new Connection(endpoint, configuration, log);
        }

        /// <inheritdoc />
        public Task<Endpoint> ResolveAsync(Uri uri, ILog log)
        {
            return Endpoint.ResolveAsync(uri, log);
        }
    }
}