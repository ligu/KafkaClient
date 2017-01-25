using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Protocol;

namespace KafkaClient.Connections
{
    public interface IConnection : IAsyncDisposable
    {
        /// <summary>
        /// The unique ip/port endpoint of this connection.
        /// </summary>
        Endpoint Endpoint { get; }

        /// <summary>
        /// Send a specific IRequest to the connected endpoint.
        /// </summary>
        /// <typeparam name="T">The type of the KafkaResponse expected from the request being sent.</typeparam>
        /// <param name="request">The Request to send to the connected endpoint.</param>
        /// <param name="cancellationToken">The token for cancelling the send request.</param>
        /// <param name="context">The context for the request.</param>
        /// <returns>Task representing the future responses from the sent request.</returns>
        Task<T> SendAsync<T>(IRequest<T> request, CancellationToken cancellationToken, IRequestContext context = null) where T : class, IResponse;
    }
}