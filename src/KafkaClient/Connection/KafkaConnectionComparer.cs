using System.Collections.Generic;

namespace KafkaClient.Connection
{
    public class KafkaConnectionComparer : IEqualityComparer<IKafkaConnection>
    {
        public static readonly KafkaConnectionComparer Singleton = new KafkaConnectionComparer();

        public bool Equals(IKafkaConnection x, IKafkaConnection y)
        {
            if (ReferenceEquals(null, x)) return ReferenceEquals(null, y);
            if (ReferenceEquals(null, y)) return false;
            if (ReferenceEquals(x, y)) return true;

            return x.Endpoint == y.Endpoint;
        }

        public int GetHashCode(IKafkaConnection obj)
        {
            return obj?.Endpoint.GetHashCode() ?? 0;
        }
    }

    public static class Extensions
    {
        public static IKafkaConnection Create(this KafkaOptions options, KafkaEndpoint endpoint)
        {
            return options.KafkaConnectionFactory.Create(endpoint, options.ResponseTimeoutMs, options.Log, options.MaxRetry, options.MaximumReconnectionTimeout, options.TrackTelemetry);
        }
    }
}