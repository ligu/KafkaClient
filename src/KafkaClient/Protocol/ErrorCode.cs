// ReSharper disable InconsistentNaming -- naming same as http://kafka.apache.org/protocol.html#protocol_api_keys for easy googling
namespace KafkaClient.Protocol
{
    /// <summary>
    /// Enumeration of error codes that might be returned from a Kafka server
    /// 
    /// See http://kafka.apache.org/protocol.html#protocol_error_codes for details.
    /// </summary>
    public enum ErrorCode : short
    {
        /// <summary>
        /// No error -- it worked!
        /// </summary>
        NONE = 0,

        /// <summary>
        /// The server experienced an unexpected error when processing the request
        /// </summary>
        UNKNOWN = -1,

        /// <summary>
        /// The requested offset is not within the range of offsets maintained by the server.
        /// </summary>
        OFFSET_OUT_OF_RANGE = 1,

        /// <summary>
        /// This message has failed its CRC checksum, exceeds the valid size, or is otherwise corrupt.
        /// </summary>
        CORRUPT_MESSAGE = 2,

        /// <summary>
        /// This server does not host this topic-partition.
        /// </summary>
        UNKNOWN_TOPIC_OR_PARTITION = 3,

        /// <summary>
        /// The requested fetch size is invalid (negative?).
        /// </summary>
        INVALID_FETCH_SIZE = 4,

        /// <summary>
        /// There is no leader for this topic-partition as we are in the middle of a leadership election.
        /// </summary>
        LEADER_NOT_AVAILABLE = 5,

        /// <summary>
        /// This server is not the leader for that topic-partition. It indicates that the clients metadata is out of date.
        /// </summary>
        NOT_LEADER_FOR_PARTITION = 6,

        /// <summary>
        /// The request timed out (on the server).
        /// </summary>
        REQUEST_TIMED_OUT = 7,

        /// <summary>
        /// Internal error code for broker-to-broker communication: The broker is not available.
        /// </summary>
        BROKER_NOT_AVAILABLE = 8,

        /// <summary>
        /// The replica is not available for the requested topic-partition.
        /// </summary>
        REPLICA_NOT_AVAILABLE = 9,

        /// <summary>
        /// The request included a message larger than the max message size the server will accept.
        /// </summary>
        MESSAGE_TOO_LARGE = 10,

        /// <summary>
        /// Internal error code for broker-to-broker communication: The controller moved to another broker.
        /// </summary>
        STALE_CONTROLLER_EPOCH = 11,

        /// <summary>
        /// The metadata field of the offset request was too large.
        /// </summary>
        OFFSET_METADATA_TOO_LARGE = 12,

        /// <summary>
        /// The server disconnected before a response was received.
        /// </summary>
        NETWORK_EXCEPTION = 13,

        /// <summary>
        /// The coordinator is loading and hence can't process requests for this group.
        /// </summary>
        GROUP_LOAD_IN_PROGRESS = 14,

        /// <summary>
        /// The group coordinator is not available.
        /// </summary>
        GROUP_COORDINATOR_NOT_AVAILABLE = 15,

        /// <summary>
        /// This is not the correct coordinator for this group.
        /// </summary>
        NOT_COORDINATOR_FOR_GROUP = 16,

        /// <summary>
        /// The request attempted to perform an operation on an invalid topic.
        /// </summary>
        INVALID_TOPIC_EXCEPTION = 17,

        /// <summary>
        /// The request included message batch larger than the configured segment size on the server.
        /// </summary>
        RECORD_LIST_TOO_LARGE = 18,

        /// <summary>
        /// Messages are rejected since there are fewer in-sync replicas than required.
        /// </summary>
        NOT_ENOUGH_REPLICAS = 19,

        /// <summary>
        /// Messages are written to the log, but to fewer in-sync replicas than required.
        /// </summary>
        NOT_ENOUGH_REPLICAS_AFTER_APPEND = 20,

        /// <summary>
        /// Produce request specified an invalid value for required acks.
        /// </summary>
        INVALID_REQUIRED_ACKS = 21,

        /// <summary>
        /// Specified group generation id is not valid (not current).
        /// </summary>
        ILLEGAL_GENERATION = 22,

        /// <summary>
        /// The group member's supported protocols are incompatible with those of existing members.
        /// </summary>
        INCONSISTENT_GROUP_PROTOCOL = 23,

        /// <summary>
        /// The configured groupId is invalid (empty or null).
        /// </summary>
        INVALID_GROUP_ID = 24,

        /// <summary>
        /// The memberId is not in the current generation (on group requests).
        /// </summary>
        UNKNOWN_MEMBER_ID = 25,

        /// <summary>
        /// The session timeout is not within the range allowed by the broker (as configured
        /// by group.min.session.timeout.ms and group.max.session.timeout.ms).
        /// </summary>
        INVALID_SESSION_TIMEOUT = 26,

        /// <summary>
        /// The group is rebalancing, so a rejoin is needed.
        /// </summary>
        REBALANCE_IN_PROGRESS = 27,

        /// <summary>
        /// The committing offset data size is not valid
        /// </summary>
        INVALID_COMMIT_OFFSET_SIZE = 28,

        /// <summary>
        /// Not authorized to access topic.
        /// </summary>
        TOPIC_AUTHORIZATION_FAILED = 29,

        /// <summary>
        /// Not authorized to access group.
        /// </summary>
        GROUP_AUTHORIZATION_FAILED = 30,

        /// <summary>
        /// Cluster authorization failed.
        /// </summary>
        CLUSTER_AUTHORIZATION_FAILED = 31,

        /// <summary>
        /// The timestamp of the message is out of acceptable range.
        /// </summary>
        INVALID_TIMESTAMP = 32,

        /// <summary>
        /// The broker does not support the requested SASL mechanism.
        /// </summary>
        UNSUPPORTED_SASL_MECHANISM = 33,

        /// <summary>
        /// Request is not valid given the current SASL state.
        /// </summary>
        ILLEGAL_SASL_STATE = 34,

        /// <summary>
        /// The version of API is not supported.
        /// </summary>
        UNSUPPORTED_VERSION = 35,

        /// <summary>
        /// Topic with this name already exists.
        /// </summary>
        TOPIC_ALREADY_EXISTS = 36,

        /// <summary>
        /// Number of partitions is invalid.
        /// </summary>
        INVALID_PARTITIONS = 37,

        /// <summary>
        /// Replication-factor is invalid.
        /// </summary>
        INVALID_REPLICATION_FACTOR = 38,

        /// <summary>
        /// Replica assignment is invalid.
        /// </summary>
        INVALID_REPLICA_ASSIGNMENT = 39,

        /// <summary>
        /// Configuration is invalid.
        /// </summary>
        INVALID_CONFIG = 40,

        /// <summary>
        /// This is not the correct controller for this cluster.
        /// </summary>
        NOT_CONTROLLER = 41,

        /// <summary>
        /// This most likely occurs because of a request being malformed by the client library or the message 
        /// was sent to an incompatible broker. See the broker logs for more details.
        /// </summary>
        INVALID_REQUEST = 42,

        /// <summary>
        /// The message format version on the broker does not support the request.
        /// </summary>
        UNSUPPORTED_FOR_MESSAGE_FORMAT = 43
    }
}