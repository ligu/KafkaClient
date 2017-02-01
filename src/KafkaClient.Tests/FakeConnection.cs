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
    public class FakeConnection : IConnection, IEnumerable<KeyValuePair<ApiKey, Func<IRequestContext, Task<IResponse>>>>, IEnumerable<KeyValuePair<ApiKey, Func<IRequest, IRequestContext, Task<IResponse>>>>
    {
        public FakeConnection(Endpoint endpoint)
        {
            Endpoint = endpoint;
        }

        public long this[ApiKey apiKey]
        {
            get {
                long count;
                return _requestCounts.TryGetValue(apiKey, out count) ? count : 0L;
            }
            set {
                _requestCounts.AddOrUpdate(apiKey, value, (k, v) => value);
            }
        }
        private readonly ConcurrentDictionary<ApiKey, long> _requestCounts = new ConcurrentDictionary<ApiKey, long>();

        public void Add(ApiKey apiKey, Func<IRequestContext, Task<IResponse>> responseFunc)
        {
            _contextFunctions.AddOrUpdate(apiKey, responseFunc, (k, v) => responseFunc);
        }
        private readonly ConcurrentDictionary<ApiKey, Func<IRequestContext, Task<IResponse>>> _contextFunctions = new ConcurrentDictionary<ApiKey, Func<IRequestContext, Task<IResponse>>>();

        public void Add(ApiKey apiKey, Func<IRequest, IRequestContext, Task<IResponse>> responseFunc)
        {
            _requestFunctions.AddOrUpdate(apiKey, responseFunc, (k, v) => responseFunc);
        }
        private readonly ConcurrentDictionary<ApiKey, Func<IRequest, IRequestContext, Task<IResponse>>> _requestFunctions = new ConcurrentDictionary<ApiKey, Func<IRequest, IRequestContext, Task<IResponse>>>();

        public Endpoint Endpoint { get; }

        /// <exception cref="Exception">A delegate callback throws an exception.</exception>
        public async Task<T> SendAsync<T>(IRequest<T> request, CancellationToken cancellationToken, IRequestContext context = null) where T : class, IResponse
        {
            var count = (int)_requestCounts.AddOrUpdate(request.ApiKey, 1L, (type, current) => current + 1);
            context = new RequestContext(count, context?.ApiVersion, context?.ClientId, context?.Encoders, context?.ProtocolType, context?.OnProduceRequestMessages);

            Func<IRequestContext, Task<IResponse>> contextFunc;
            if (_contextFunctions.TryGetValue(request.ApiKey, out contextFunc)) {
                return (T) await contextFunc(context);
            }

            Func<IRequest, IRequestContext, Task<IResponse>> requestFunc;
            if (_requestFunctions.TryGetValue(request.ApiKey, out requestFunc)) {
                return (T) await requestFunc(request, context);
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

        IEnumerator<KeyValuePair<ApiKey, Func<IRequestContext, Task<IResponse>>>> IEnumerable<KeyValuePair<ApiKey, Func<IRequestContext, Task<IResponse>>>>.GetEnumerator()
        {
            throw new NotImplementedException();
        }

        IEnumerator<KeyValuePair<ApiKey, Func<IRequest, IRequestContext, Task<IResponse>>>> IEnumerable<KeyValuePair<ApiKey, Func<IRequest, IRequestContext, Task<IResponse>>>>.GetEnumerator()
        {
            throw new NotImplementedException();
        }

        public Task DisposeAsync()
        {
            return Task.FromResult(0);
        }

        public bool IsDisposed => false;
    }
}