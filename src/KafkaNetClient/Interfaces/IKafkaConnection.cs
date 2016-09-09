using KafkaNet.Model;
using System;
using System.Threading;
using System.Threading.Tasks;
using KafkaNet.Protocol;

namespace KafkaNet
{
    public interface IKafkaConnection : IDisposable
    {
        /// <summary>
        /// The unique endpoint location of this connection.
        /// </summary>
        KafkaEndpoint Endpoint { get; }

        /// <summary>
        /// Value indicating the read polling thread is still active.
        /// </summary>
        bool ReadPolling { get; }

        /// <summary>
        /// Send raw payload data up to the connected endpoint.
        /// </summary>
        /// <param name="payload">The raw data to send to the connected endpoint.</param>
        /// <param name="token">The token for cancelling the send request.</param>
        /// <returns>Task representing the future success or failure of query.</returns>
        Task SendAsync(KafkaDataPayload payload, CancellationToken token);

        /// <summary>
        /// Send a specific IKafkaRequest to the connected endpoint.
        /// </summary>
        /// <typeparam name="T">The type of the KafkaResponse expected from the request being sent.</typeparam>
        /// <param name="request">The KafkaRequest to send to the connected endpoint.</param>
        /// <param name="context">The context for the request.</param>
        /// <returns>Task representing the future responses from the sent request.</returns>
        Task<T> SendAsync<T>(IKafkaRequest<T> request, IRequestContext context = null) where T : class, IKafkaResponse;
    }
}