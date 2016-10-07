using System;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetFetchRequest
    /// The offsets for a given consumer group is maintained by a specific broker called the offset coordinator. i.e., a consumer needs
    /// to issue its offset commit and fetch requests to this specific broker. It can discover the current offset coordinator by issuing a consumer metadata request.
    /// </summary>
    public class GroupCoordinatorRequest : Request, IRequest<GroupCoordinatorResponse>, IEquatable<GroupCoordinatorRequest>
    {
        public GroupCoordinatorRequest(string consumerGroup) 
            : base(ApiKeyRequestType.GroupCoordinator)
        {
            ConsumerGroup = consumerGroup;
        }

        public string ConsumerGroup { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as GroupCoordinatorRequest);
        }

        /// <inheritdoc />
        public bool Equals(GroupCoordinatorRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return string.Equals(ConsumerGroup, other.ConsumerGroup);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return ConsumerGroup?.GetHashCode() ?? 0;
        }

        /// <inheritdoc />
        public static bool operator ==(GroupCoordinatorRequest left, GroupCoordinatorRequest right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(GroupCoordinatorRequest left, GroupCoordinatorRequest right)
        {
            return !Equals(left, right);
        }
    }
}