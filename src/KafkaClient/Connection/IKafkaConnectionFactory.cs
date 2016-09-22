using System;
using KafkaClient.Common;

namespace KafkaClient.Connection
{
    public interface IKafkaConnectionFactory
    {
        /// <summary>
        /// Create a new KafkaConnection.
        /// </summary>
        /// <param name="endpoint">The specific KafkaEndpoint of the server to connect to.</param>
        /// <param name="options">The options for the connection (including things like connection and request timeouts).</param>
        /// <param name="log">Logging interface used to record any log messages created by the connection.</param>
        /// <returns>IKafkaConnection initialized to connecto to the given endpoint.</returns>
        IKafkaConnection Create(KafkaEndpoint endpoint, IKafkaConnectionOptions options, IKafkaLog log);

        /// <summary>
        /// Resolves a generic Uri into a uniquely identifiable KafkaEndpoint.
        /// </summary>
        /// <param name="kafkaAddress">The address to the kafka server to resolve.</param>
        /// <param name="log">Logging interface used to record any log messages created by the Resolving process.</param>
        /// <returns>KafkaEndpoint with resolved IP and Address.</returns>
        KafkaEndpoint Resolve(Uri kafkaAddress, IKafkaLog log);
    }
}