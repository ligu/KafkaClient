namespace KafkaClient.Protocol
{
    /// <summary>
    /// Enumeration of numeric codes that the ApiKey in the request can take.
    /// </summary>
    public enum ApiKey : short
    {
        Produce = 0,
        Fetch = 1,
        Offsets = 2,
        Metadata = 3,
        OffsetCommit = 8,
        OffsetFetch = 9,
        GroupCoordinator = 10,
        JoinGroup = 11,
        Heartbeat = 12,
        LeaveGroup = 13,
        SyncGroup = 14,
        DescribeGroups = 15,
        ListGroups = 16,
        SaslHandshake = 17,
        ApiVersions = 18,
        CreateTopics = 19,
        DeleteTopics = 20
    }
}