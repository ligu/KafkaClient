using System;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Protocol;

namespace KafkaClient.Connections
{
    public interface IConnection : IDisposable
    {
        /// <summary>
        /// The unique ip/port endpoint of this connection.
        /// </summary>
        Endpoint Endpoint { get; }

        /// <summary>
        /// Value indicating the read polling thread is still active.
        /// </summary>
        bool IsReaderAlive { get; }

        /// <summary>
        /// Send raw byte[] payload to the kafka server with a task indicating upload is complete.
        /// </summary>
        /// <param name="payload">kafka protocol formatted byte[] payload</param>
        /// <param name="token">Cancellation token used to cancel the transfer.</param>
        /// <returns>Task which signals the completion of the upload of data to the server.</returns>
        Task SendAsync(DataPayload payload, CancellationToken token);

        /// <summary>
        /// Send a specific IRequest to the connected endpoint.
        /// </summary>
        /// <typeparam name="T">The type of the KafkaResponse expected from the request being sent.</typeparam>
        /// <param name="request">The Request to send to the connected endpoint.</param>
        /// <param name="token">The token for cancelling the send request.</param>
        /// <param name="context">The context for the request.</param>
        /// <returns>Task representing the future responses from the sent request.</returns>
        Task<T> SendAsync<T>(IRequest<T> request, CancellationToken token, IRequestContext context = null) where T : class, IResponse;
    }
}