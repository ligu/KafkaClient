using System;
using KafkaClient.Connections;

namespace KafkaClient
{
    public class GroupConnection : ServerConnection, IEquatable<GroupConnection>
    {
        public GroupConnection(string groupId, int serverId, IConnection connection)
            : base(serverId, connection)
        {
            GroupId = groupId;
        }

        public string GroupId { get; }

        public override string ToString() => $"{base.ToString()} GroupId:{GroupId}";

        #region Equality

        public override bool Equals(object obj)
        {
            return Equals(obj as GroupConnection);
        }

        public bool Equals(GroupConnection other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                && string.Equals(GroupId, other.GroupId);
        }

        public override int GetHashCode()
        {
            unchecked {
                return (base.GetHashCode() * 397) ^ (GroupId?.GetHashCode() ?? 0);
            }
        }

        #endregion
    }
}