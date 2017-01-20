using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Connections;
using KafkaClient.Protocol;
using Nito.AsyncEx;

namespace KafkaClient.Tests
{
    public class FakeConnection : IConnection, IEnumerable<KeyValuePair<ApiKeyRequestType, Func<IRequestContext, Task<IResponse>>>>, IEnumerable<KeyValuePair<ApiKeyRequestType, Func<IRequest, IRequestContext, Task<IResponse>>>>
    {
        public FakeConnection(Uri address)
        {
            Endpoint = AsyncContext.Run(() => Endpoint.ResolveAsync(address, TestConfig.Log));
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

        public void Add(ApiKeyRequestType requestType, Func<IRequestContext, Task<IResponse>> responseFunc)
        {
            _contextFunctions.AddOrUpdate(requestType, responseFunc, (k, v) => responseFunc);
        }
        private readonly ConcurrentDictionary<ApiKeyRequestType, Func<IRequestContext, Task<IResponse>>> _contextFunctions = new ConcurrentDictionary<ApiKeyRequestType, Func<IRequestContext, Task<IResponse>>>();

        public void Add(ApiKeyRequestType requestType, Func<IRequest, IRequestContext, Task<IResponse>> responseFunc)
        {
            _requestFunctions.AddOrUpdate(requestType, responseFunc, (k, v) => responseFunc);
        }
        private readonly ConcurrentDictionary<ApiKeyRequestType, Func<IRequest, IRequestContext, Task<IResponse>>> _requestFunctions = new ConcurrentDictionary<ApiKeyRequestType, Func<IRequest, IRequestContext, Task<IResponse>>>();

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

        IEnumerator<KeyValuePair<ApiKeyRequestType, Func<IRequestContext, Task<IResponse>>>> IEnumerable<KeyValuePair<ApiKeyRequestType, Func<IRequestContext, Task<IResponse>>>>.GetEnumerator()
        {
            throw new NotImplementedException();
        }

        IEnumerator<KeyValuePair<ApiKeyRequestType, Func<IRequest, IRequestContext, Task<IResponse>>>> IEnumerable<KeyValuePair<ApiKeyRequestType, Func<IRequest, IRequestContext, Task<IResponse>>>>.GetEnumerator()
        {
            throw new NotImplementedException();
        }
    }
}