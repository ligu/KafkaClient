using System;
using System.Threading.Tasks;

namespace KafkaClient.Common
{
    public interface IAsyncDisposable : IDisposable
    {
        Task Disposal { get; }
    }
}