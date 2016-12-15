using System;
using KafkaClient.Connections;

namespace KafkaClient
{
    public class GroupBroker : IEquatable<GroupBroker>
    {
        public GroupBroker(string groupId, int brokerId, IConnection connection)
        {
            GroupId = groupId;
            BrokerId = brokerId;
            Connection = connection;
        }

        public IConnection Connection { get; }

        public int BrokerId { get; }

        public string GroupId { get; }

        public override string ToString() => $"{Connection.Endpoint.ServerUri} ({BrokerId}) group/{GroupId}";

        public override bool Equals(object obj)
        {
            return Equals(obj as GroupBroker);
        }

        public bool Equals(GroupBroker other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return BrokerId == other.BrokerId && string.Equals(GroupId, other.GroupId);
        }

        public override int GetHashCode()
        {
            unchecked {
                return (BrokerId * 397) ^ (GroupId?.GetHashCode() ?? 0);
            }
        }

        public static bool operator ==(GroupBroker left, GroupBroker right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(GroupBroker left, GroupBroker right)
        {
            return !Equals(left, right);
        }
    }
}