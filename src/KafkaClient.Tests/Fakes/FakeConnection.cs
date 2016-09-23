using System;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Connection;
using KafkaClient.Protocol;

namespace KafkaClient.Tests.Fakes
{
    public class FakeConnection : IConnection
    {
        public Func<Task<ProduceResponse>> ProduceResponseFunction;
        public Func<Task<MetadataResponse>> MetadataResponseFunction;
        public Func<Task<OffsetResponse>> OffsetResponseFunction;
        public Func<Task<FetchResponse>> FetchResponseFunction;

        public FakeConnection(Uri address)
        {
            Endpoint = new ConnectionFactory().Resolve(address, new TraceLog());
        }

        public long MetadataRequestCallCount; // { get; set; }
        public long ProduceRequestCallCount; //{ get; set; }
        public long OffsetRequestCallCount; //{ get; set; }
        public long FetchRequestCallCount; // { get; set; }

        public Endpoint Endpoint { get; }

        public bool IsReaderAlive => true;

        public Task SendAsync(DataPayload payload, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        /// <exception cref="Exception">A delegate callback throws an exception.</exception>
        public async Task<T> SendAsync<T>(IRequest<T> request, CancellationToken token, IRequestContext context = null) where T : class, IResponse
        {
            T result;

            if (typeof(T) == typeof(ProduceResponse))
            {
                Interlocked.Increment(ref ProduceRequestCallCount);
                result = (T)((object)await ProduceResponseFunction());
            }
            else if (typeof(T) == typeof(MetadataResponse))
            {
                Interlocked.Increment(ref MetadataRequestCallCount);
                result = (T)(object)await MetadataResponseFunction();
            }
            else if (typeof(T) == typeof(OffsetResponse))
            {
                Interlocked.Increment(ref OffsetRequestCallCount);
                result = (T)(object)await OffsetResponseFunction();
            }
            else if (typeof(T) == typeof(FetchResponse))
            {
                Interlocked.Increment(ref FetchRequestCallCount);
                result = (T)(object)await FetchResponseFunction();
            }
            else
            {
                throw new NotImplementedException(typeof(T).FullName);
            }
            return result;
        }

        public void Dispose()
        {
        }
    }
}