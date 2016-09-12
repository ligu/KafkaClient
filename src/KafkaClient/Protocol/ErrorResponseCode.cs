namespace KafkaClient.Protocol
{
    /// <summary>
    /// Enumeration of error codes that might be returned from a Kafka server
    /// </summary>
    public enum ErrorResponseCode : short
    {
        /// <summary>
        /// No error--it worked!
        /// </summary>
        NoError = 0,

        /// <summary>
        /// An unexpected server error
        /// </summary>
        Unknown = -1,

        /// <summary>
        /// The requested offset is outside the range of offsets maintained by the server for the given topic/partition.
        /// </summary>
        OffsetOutOfRange = 1,

        /// <summary>
        /// This indicates that a message contents does not match its CRC
        /// </summary>
        InvalidMessage = 2,

        /// <summary>
        /// This request is for a topic or partition that does not exist on this broker.
        /// </summary>
        UnknownTopicOrPartition = 3,

        /// <summary>
        /// The message has a negative size
        /// </summary>
        InvalidMessageSize = 4,

        /// <summary>
        /// This error is thrown if we are in the middle of a leadership election and there is currently no leader for this partition and hence it is unavailable for writes.
        /// </summary>
        LeaderNotAvailable = 5,

        /// <summary>
        /// This error is thrown if the client attempts to send messages to a replica that is not the leader for some partition. It indicates that the clients metadata is out of date.
        /// </summary>
        NotLeaderForPartition = 6,

        /// <summary>
        /// This error is thrown if the request exceeds the user-specified time limit in the request.
        /// </summary>
        RequestTimedOut = 7,

        /// <summary>
        /// This is not a client facing error and is used only internally by intra-cluster broker communication.
        /// </summary>
        BrokerNotAvailable = 8,

        /// <summary>
        /// If replica is expected on a broker, but is not.
        /// </summary>
        ReplicaNotAvailable = 9,

        /// <summary>
        /// The server has a configurable maximum message size to avoid unbounded memory allocation. This error is thrown if the client attempt to produce a message larger than this maximum.
        /// </summary>
        MessageSizeTooLarge = 10,

        /// <summary>
        /// Internal error code for broker-to-broker communication.
        /// </summary>
        StaleControllerEpoch = 11,

        /// <summary>
        /// If you specify a string larger than configured maximum for offset metadata
        /// </summary>
        OffsetMetadataTooLarge = 12,

        /// <summary>
        /// The server disconnected before a response was received.
        /// </summary>
        NetworkException = 13,

        /// <summary>
        /// The broker returns this error code for an offset fetch request if it is still loading offsets (after a leader change for that offsets topic partition).
        /// </summary>
        OffsetsLoadInProgress = 14,

        /// <summary>
        /// The broker returns this error code for consumer metadata requests or offset commit requests if the offsets topic has not yet been created.
        /// </summary>
        ConsumerCoordinatorNotAvailable = 15,

        /// <summary>
        /// The broker returns this error code if it receives an offset fetch or commit request for a consumer group that it is not a coordinator for.
        /// </summary>
        NotCoordinatorForConsumer = 16,

        /// <summary>
        /// The request attempted to perform an operation on an invalid topic.
        /// </summary>
        InvalidTopic = 17,

        /// <summary>
        /// The request included message batch larger than the configured segment size on the server.
        /// </summary>
        RecordListTooLarge = 18,

        /// <summary>
        /// Messages are rejected since there are fewer in-sync replicas than required.
        /// </summary>
        NotEnoughReplicas = 19,

        /// <summary>
        /// Messages are written to the log, but to fewer in-sync replicas than required.
        /// </summary>
        NotEnoughReplicasAfterAppend = 20,

        /// <summary>
        /// Produce request specified an invalid value for required acks.
        /// </summary>
        InvalidRequiredAcks = 21,

        /// <summary>
        /// Specified group generation id is not valid.
        /// </summary>
        IllegalGeneration = 22,

        /// <summary>
        /// The group member's supported protocols are incompatible with those of existing members.
        /// </summary>
        InconsistentGroupProtocol = 23,

        /// <summary>
        /// The configured groupId is invalid.
        /// </summary>
        InvalidGroupId = 24,

        /// <summary>
        /// The coordinator is not aware of this member.
        /// </summary>
        UnknownMemberId = 25,

        /// <summary>
        /// The session timeout is not within the range allowed by the broker (as configured
        /// by group.min.session.timeout.ms and group.max.session.timeout.ms).
        /// </summary>
        InvalidSessionTimeout = 26,

        /// <summary>
        /// The group is rebalancing, so a rejoin is needed.
        /// </summary>
        RebalanceInProgress = 27,

        /// <summary>
        /// The committing offset data size is not valid
        /// </summary>
        InvalidCommitOffsetSize = 28,

        /// <summary>
        /// Not authorized to access topic.
        /// </summary>
        TopicAuthorizationFailed = 29,

        /// <summary>
        /// Not authorized to access group.
        /// </summary>
        GroupAuthorizationFailed = 30,

        /// <summary>
        /// Cluster authorization failed.
        /// </summary>
        ClusterAuthorizationFailed = 31,

        /// <summary>
        /// The timestamp of the message is out of acceptable range.
        /// </summary>
        InvalidTimestamp = 32,

        /// <summary>
        /// The broker does not support the requested SASL mechanism.
        /// </summary>
        UnsupportedSaslMechanism = 33,

        /// <summary>
        /// Request is not valid given the current SASL state.
        /// </summary>
        IllegalSaslState = 34,

        /// <summary>
        /// The version of API is not supported.
        /// </summary>
        UnsupportedVersion = 35
    }
}