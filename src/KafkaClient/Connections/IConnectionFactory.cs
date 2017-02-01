using KafkaClient.Common;

namespace KafkaClient.Connections
{
    public interface IConnectionFactory
    {
        /// <summary>
        /// Create a new Connection.
        /// </summary>
        /// <param name="endpoint">The specific Endpoint of the server to connect to.</param>
        /// <param name="configuration">The configuration for the connection (including things like connection and request timeouts).</param>
        /// <param name="log">Logging interface used to record any log messages created by the connection.</param>
        /// <returns>IConnection initialized to connecto to the given endpoint.</returns>
        IConnection Create(Endpoint endpoint, IConnectionConfiguration configuration, ILog log = null);
    }
}