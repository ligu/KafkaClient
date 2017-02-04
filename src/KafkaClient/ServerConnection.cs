using System;
using KafkaClient.Connections;

namespace KafkaClient
{
    public class ServerConnection : IEquatable<ServerConnection>
    {
        protected ServerConnection(int serverId, IConnection connection)
        {
            ServerId = serverId;
            Connection = connection;
        }

        public IConnection Connection { get; }

        public int ServerId { get; }

        public override string ToString() => $"{Connection.Endpoint} ({ServerId})";

        #region Equality

        public override bool Equals(object obj)
        {
            return Equals(obj as ServerConnection);
        }

        public bool Equals(ServerConnection other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return ServerId == other.ServerId;
        }

        public override int GetHashCode()
        {
            return ServerId;
        }

        #endregion
    }
}