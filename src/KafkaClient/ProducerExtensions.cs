using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Protocol;

namespace KafkaClient
{
    public static class ProducerExtensions
    {
        public static async Task<IProducer> CreateProducerAsync(this KafkaOptions options)
        {
            return new Producer(await options.CreateRouterAsync(), options.ProducerConfiguration, false);
        }

        /// <summary>
        /// Send a message to the given topic.
        /// </summary>
        /// <param name="producer">The message producer</param>
        /// <param name="messages">The messages to send.</param>
        /// <param name="topicName">The name of the kafka topic to send the messages to.</param>
        /// <param name="partitionId">The partition to send messages to</param>
        /// <param name="cancellationToken"></param>
        /// <returns>List of ProduceTopic response from each partition sent to or empty list if acks = 0.</returns>
        public static Task<ProduceResponse.Topic> SendMessagesAsync(this IProducer producer, IEnumerable<Message> messages, string topicName, int partitionId, CancellationToken cancellationToken)
        {
            return producer.SendMessagesAsync(messages, topicName, partitionId, null, cancellationToken);
        }

        /// <summary>
        /// Send a message to the given topic.
        /// </summary>
        /// <param name="producer">The message producer</param>
        /// <param name="message">The message to send.</param>
        /// <param name="topicName">The name of the kafka topic to send the messages to.</param>
        /// <param name="partitionId">The partition to send messages to</param>
        /// <param name="cancellationToken"></param>
        /// <returns>List of ProduceTopic response from each partition sent to or empty list if acks = 0.</returns>
        public static Task<ProduceResponse.Topic> SendMessageAsync(this IProducer producer, Message message, string topicName, int partitionId, CancellationToken cancellationToken)
        {
            return producer.SendMessageAsync(message, topicName, partitionId, null, cancellationToken);
        }

        /// <summary>
        /// Send a message to the given topic.
        /// </summary>
        /// <param name="producer">The message producer</param>
        /// <param name="message">The message to send.</param>
        /// <param name="topicName">The name of the kafka topic to send the messages to.</param>
        /// <param name="partitionId">The partition to send messages to, or <value>null</value> for any.</param>
        /// <param name="configuration">The configuration for sending the messages (ie acks, ack Timeout and codec)</param>
        /// <param name="cancellationToken">The token for cancellation</param>
        /// <returns>List of ProduceTopic response from each partition sent to or empty list if acks = 0.</returns>
        public static Task<ProduceResponse.Topic> SendMessageAsync(this IProducer producer, Message message, string topicName, int partitionId, ISendMessageConfiguration configuration, CancellationToken cancellationToken)
        {
            return producer.SendMessagesAsync(new[] { message }, topicName, partitionId, configuration, cancellationToken);
        }
    }
}