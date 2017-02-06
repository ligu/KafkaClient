﻿using System;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// Also called a Broker or Node
    /// </summary>
    public class Server : IEquatable<Server>
    {
        public Server(int id, string host, int port, string rack = null)
        {
            Id = id;
            Host = host;
            Port = port;
            Rack = rack;
        }

        public int Id { get; }
        public string Host { get; }
        public int Port { get; }
        public string Rack { get; }

        #region Equality

        public override bool Equals(object obj)
        {
            return Equals(obj as Server);
        }

        public bool Equals(Server other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Id == other.Id 
                   && string.Equals(Host, other.Host) 
                   && Port == other.Port
                   && string.Equals(Rack, other.Rack);
        }

        public override int GetHashCode()
        {
            unchecked {
                var hashCode = Id;
                hashCode = (hashCode*397) ^ (Host?.GetHashCode() ?? 0);
                hashCode = (hashCode*397) ^ Port;
                hashCode = (hashCode*397) ^ (Rack?.GetHashCode() ?? 0);
                return hashCode;
            }
        }
                
        #endregion

        public override string ToString() => $"{{NodeId:{Id},Host:'{Host}',Port:{Port},Rack:{Rack}}}";
    }
}