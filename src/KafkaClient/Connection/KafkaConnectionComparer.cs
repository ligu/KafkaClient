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
}