namespace KafkaClient.Protocol
{
    /// <summary>
    /// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetFetchRequest
    /// The offsets for a given consumer group is maintained by a specific broker called the offset coordinator. i.e., a consumer needs
    /// to issue its offset commit and fetch requests to this specific broker. It can discover the current offset coordinator by issuing a consumer metadata request.
    /// </summary>
    public class GroupCoordinatorRequest : KafkaRequest, IKafkaRequest<GroupCoordinatorResponse>
    {
        public GroupCoordinatorRequest(string consumerGroup) 
            : base(ApiKeyRequestType.GroupCoordinator)
        {
            ConsumerGroup = consumerGroup;
        }

        public string ConsumerGroup { get; }
    }
}