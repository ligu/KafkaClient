using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Connections;
using KafkaClient.Protocol;

namespace KafkaClient.Tests
{
    public class FakeConnection : IConnection, IEnumerable<KeyValuePair<ApiKeyRequestType, Func<Task<IResponse>>>>
    {
        public FakeConnection(Uri address)
        {
            Endpoint = new ConnectionFactory().Resolve(address, TestConfig.Log);
        }

        public long this[ApiKeyRequestType requestType]
        {
            get {
                long count;
                return _requestCounts.TryGetValue(requestType, out count) ? count : 0L;
            }
            set {
                _requestCounts.AddOrUpdate(requestType, value, (k, v) => value);
            }
        }
        private readonly ConcurrentDictionary<ApiKeyRequestType, long> _requestCounts = new ConcurrentDictionary<ApiKeyRequestType, long>();

        public void Add(ApiKeyRequestType requestType, Func<Task<IResponse>> responseFunc)
        {
            _responseFunctions.AddOrUpdate(requestType, responseFunc, (k, v) => responseFunc);
        }
        private readonly ConcurrentDictionary<ApiKeyRequestType, Func<Task<IResponse>>> _responseFunctions = new ConcurrentDictionary<ApiKeyRequestType, Func<Task<IResponse>>>();

        public Endpoint Endpoint { get; }

        /// <exception cref="Exception">A delegate callback throws an exception.</exception>
        public async Task<T> SendAsync<T>(IRequest<T> request, CancellationToken token, IRequestContext context = null) where T : class, IResponse
        {
            var count = _requestCounts.AddOrUpdate(request.ApiKey, 1L, (type, current) => current + 1);

            Func<Task<IResponse>> responseFunc;
            if (_responseFunctions.TryGetValue(request.ApiKey, out responseFunc)) {
                return (T) await responseFunc();
            }

            throw new NotImplementedException(typeof(T).FullName);
        }

        public void Dispose()
        {
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            throw new NotImplementedException();
        }

        IEnumerator<KeyValuePair<ApiKeyRequestType, Func<Task<IResponse>>>> IEnumerable<KeyValuePair<ApiKeyRequestType, Func<Task<IResponse>>>>.GetEnumerator()
        {
            throw new NotImplementedException();
        }
    }
}