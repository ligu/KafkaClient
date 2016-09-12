using System;

namespace KafkaClient.Protocol
{
    public class Broker : IEquatable<Broker>
    {
        public Broker(int brokerId, string host, int port)
        {
            BrokerId = brokerId;
            Host = host;
            Port = port;
        }

        public int BrokerId { get; }
        public string Host { get; }
        public int Port { get; }
        public Uri Address => new Uri($"http://{Host}:{Port}");

        #region Equality

        public override bool Equals(object obj)
        {
            return Equals(obj as Broker);
        }

        public bool Equals(Broker other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return BrokerId == other.BrokerId 
                   && string.Equals(Host, other.Host) 
                   && Port == other.Port;
        }

        public override int GetHashCode()
        {
            unchecked {
                var hashCode = BrokerId;
                hashCode = (hashCode*397) ^ (Host?.GetHashCode() ?? 0);
                hashCode = (hashCode*397) ^ Port;
                return hashCode;
            }
        }

        public static bool operator ==(Broker left, Broker right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(Broker left, Broker right)
        {
            return !Equals(left, right);
        }
                
        #endregion

        public override string ToString() => $"http://{Host}:{Port} Broker:{BrokerId}";
    }
}