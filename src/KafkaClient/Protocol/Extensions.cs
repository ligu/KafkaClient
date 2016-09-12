using System;
using System.Collections.Generic;
using System.Linq;
using KafkaNet.Model;

namespace KafkaNet.Protocol
{
    public static class Extensions
    {
        public static Exception ExtractExceptions<TRequest, TResponse>(this TRequest request, TResponse response, KafkaEndpoint endpoint = null) 
            where TRequest : IKafkaRequest
            where TResponse : IKafkaResponse
        {
            var exceptions = new List<Exception>();
            foreach (var errorCode in response.Errors.Where(e => e != ErrorResponseCode.NoError)) {
                exceptions.Add(ExtractException(request, errorCode, endpoint));
            }
            if (exceptions.Count == 0) return new KafkaRequestException(request.ApiKey, ErrorResponseCode.NoError) { Endpoint = endpoint };
            if (exceptions.Count == 1) return exceptions[0];
            return new AggregateException(exceptions);
        }

        public static Exception ExtractException(this IKafkaRequest request, ErrorResponseCode errorCode, KafkaEndpoint endpoint) 
        {
            var exception = ExtractFetchException(request as FetchRequest, errorCode) ??
                            new KafkaRequestException(request.ApiKey, errorCode);
            exception.Endpoint = endpoint;
            return exception;
        }

        private static FetchOutOfRangeException ExtractFetchException(FetchRequest request, ErrorResponseCode errorCode)
        {
            if (errorCode == ErrorResponseCode.OffsetOutOfRange && request?.Fetches?.Count == 1) {
                var fetch = request.Fetches.First();
                return new FetchOutOfRangeException(fetch, request.ApiKey, errorCode);
            }
            return null;
        } 

        private static readonly DateTime UnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public static long ToUnixEpochMilliseconds(this DateTime pointInTime)
        {
            return pointInTime > UnixEpoch ? (long)(pointInTime - UnixEpoch).TotalMilliseconds : 0L;
        }

        public static DateTime FromUnixEpochMilliseconds(this long milliseconds)
        {
            return UnixEpoch.AddMilliseconds(milliseconds);
        }

        public static IRequestContext WithCorrelation(this IRequestContext context, int correlationId)
        {
            return new RequestContext(correlationId, context?.ApiVersion, context?.ClientId);
        }
    }
}
