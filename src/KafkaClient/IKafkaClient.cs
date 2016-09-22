using System;

namespace KafkaClient
{
    /// <summary>
    /// Common interface for consumer and producer.
    /// </summary>
    internal interface IKafkaClient : IDisposable
    {
        /// <summary>
        /// The broker router used to route requests.
        /// </summary>
        IBrokerRouter BrokerRouter { get; }
    }
}