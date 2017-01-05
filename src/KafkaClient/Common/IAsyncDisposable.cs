using System;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaClient.Common
{
    public interface IAsyncDisposable : IDisposable
    {
        Task DisposeAsync(CancellationToken cancellationToken);
    }
}