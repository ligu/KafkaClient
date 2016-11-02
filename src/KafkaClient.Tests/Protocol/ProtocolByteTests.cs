using System;
using System.Collections.Generic;
using System.Linq;
using KafkaClient.Common;
using KafkaClient.Protocol;
using KafkaClient.Protocol.Types;
using NUnit.Framework;
using NUnit.Framework.Internal;

namespace KafkaClient.Tests.Protocol
{
    /// <summary>
    /// From http://kafka.apache.org/protocol.html#protocol_types
    /// The protocol is built out of the following primitive types.
    ///
    /// Fixed Width Primitives:
    /// int8, int16, int32, int64 - Signed integers with the given precision (in bits) stored in big endian order.
    ///
    /// Variable Length Primitives:
    /// bytes, string - These types consist of a signed integer giving a length N followed by N bytes of content. 
    /// A length of -1 indicates null. string uses an int16 for its size, and bytes uses an int32.
    ///
    /// Arrays:
    /// This is a notation for handling repeated structures. These will always be encoded as an int32 size containing 
    /// the length N followed by N repetitions of the structure which can itself be made up of other primitive types. 
    /// In the BNF grammars below we will show an array of a structure foo as [foo].
    /// 
    /// Message formats are from https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-CommonRequestandResponseStructure
    /// 
    /// RequestOrResponse => Size (RequestMessage | ResponseMessage)
    ///  Size => int32    : The Size field gives the size of the subsequent request or response message in bytes. 
    ///                     The client can read requests by first reading this 4 byte size as an integer N, and 
    ///                     then reading and parsing the subsequent N bytes of the request.
    /// 
    /// Request Header => api_key api_version correlation_id client_id 
    ///  api_key => INT16             -- The id of the request type.
    ///  api_version => INT16         -- The version of the API.
    ///  correlation_id => INT32      -- A user-supplied integer value that will be passed back with the response.
    ///  client_id => NULLABLE_STRING -- A user specified identifier for the client making the request.
    /// 
    /// Response Header => correlation_id 
    ///  correlation_id => INT32      -- The user-supplied value passed in with the request
    /// </summary>
    [TestFixture]
    [Category("Unit")]
    public class ProtocolByteTests
    {
        private readonly Randomizer _randomizer = new Randomizer();

        /// <summary>
        /// ProduceRequest => RequiredAcks Timeout [TopicName [Partition MessageSetSize MessageSet]]
        ///  RequiredAcks => int16   -- This field indicates how many acknowledgements the servers should receive before responding to the request. 
        ///                             If it is 0 the server will not send any response (this is the only case where the server will not reply to 
        ///                             a request). If it is 1, the server will wait the data is written to the local log before sending a response. 
        ///                             If it is -1 the server will block until the message is committed by all in sync replicas before sending a response.
        ///  Timeout => int32        -- This provides a maximum time in milliseconds the server can await the receipt of the number of acknowledgements 
        ///                             in RequiredAcks. The timeout is not an exact limit on the request time for a few reasons: (1) it does not include 
        ///                             network latency, (2) the timer begins at the beginning of the processing of this request so if many requests are 
        ///                             queued due to server overload that wait time will not be included, (3) we will not terminate a local write so if 
        ///                             the local write time exceeds this timeout it will not be respected. To get a hard timeout of this type the client 
        ///                             should use the socket timeout.
        ///  TopicName => string     -- The topic that data is being published to.
        ///  Partition => int32      -- The partition that data is being published to.
        ///  MessageSetSize => int32 -- The size, in bytes, of the message set that follows.
        /// 
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets
        /// </summary>
        [Test]
        public void ProduceRequest(
            [Values(0, 1, 2)] short version,
            [Values(0, 2, -1)] short acks, 
            [Values(0, 1000)] int timeoutMilliseconds, 
            [Values("test", "a really long name, with spaces and punctuation!")] string topic, 
            [Values(1, 10)] int topicsPerRequest, 
            [Values(1, 5)] int totalPartitions, 
            [Values(3)] int messagesPerSet)
        {
            var payloads = new List<ProduceRequest.Payload>();
            for (var t = 0; t < topicsPerRequest; t++) {
                var partition = 1 + t%totalPartitions;
                payloads.Add(new ProduceRequest.Payload(topic + t, partition, GenerateMessages(messagesPerSet, (byte) (version >= 2 ? 1 : 0), partition)));
            }
            var request = new ProduceRequest(payloads, TimeSpan.FromMilliseconds(timeoutMilliseconds), acks);

            request.AssertCanEncodeDecodeRequest(version);
        }

        /// <summary>
        /// ProduceResponse => [TopicName [Partition ErrorCode Offset *Timestamp]] *ThrottleTime
        ///  *ThrottleTime is only version 1 (0.9.0) and above
        ///  *Timestamp is only version 2 (0.10.0) and above
        ///  TopicName => string   -- The topic this response entry corresponds to.
        ///  Partition => int32    -- The partition this response entry corresponds to.
        ///  ErrorCode => int16    -- The error from this partition, if any. Errors are given on a per-partition basis because a given partition may be 
        ///                           unavailable or maintained on a different host, while others may have successfully accepted the produce request.
        ///  Offset => int64       -- The offset assigned to the first message in the message set appended to this partition.
        ///  Timestamp => int64    -- If LogAppendTime is used for the topic, this is the timestamp assigned by the broker to the message set. 
        ///                           All the messages in the message set have the same timestamp.
        ///                           If CreateTime is used, this field is always -1. The producer can assume the timestamp of the messages in the 
        ///                           produce request has been accepted by the broker if there is no error code returned.
        ///                           Unit is milliseconds since beginning of the epoch (midnight Jan 1, 1970 (UTC)).
        ///  ThrottleTime => int32 -- Duration in milliseconds for which the request was throttled due to quota violation. 
        ///                           (Zero if the request did not violate any quota).
        /// 
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets
        /// </summary>
        [Test]
        public void ProduceResponse(
            [Values(0, 1, 2)] short version,
            [Values(-1, 0, 10000000)] long timestampMilliseconds, 
            [Values("test", "a really long name, with spaces and punctuation!")] string topicName, 
            [Values(1, 10)] int topicsPerRequest, 
            [Values(1, 5)] int totalPartitions, 
            [Values(
                ErrorResponseCode.None,
                ErrorResponseCode.CorruptMessage
            )] ErrorResponseCode errorCode,
            [Values(0, 100000)] int throttleTime)
        {
            var topics = new List<ProduceResponse.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                topics.Add(new ProduceResponse.Topic(topicName + t, t % totalPartitions, errorCode, _randomizer.Next(), version >= 2 ? timestampMilliseconds.FromUnixEpochMilliseconds() : (DateTime?)null));
            }
            var response = new ProduceResponse(topics, version >= 1 ? TimeSpan.FromMilliseconds(throttleTime) : (TimeSpan?)null);

            response.AssertCanEncodeDecodeResponse(version);
        }

        /// <summary>
        /// FetchRequest => ReplicaId MaxWaitTime MinBytes [TopicName [Partition FetchOffset MaxBytes]]
        ///  ReplicaId => int32   -- The replica id indicates the node id of the replica initiating this request. Normal client consumers should always 
        ///                          specify this as -1 as they have no node id. Other brokers set this to be their own node id. The value -2 is accepted 
        ///                          to allow a non-broker to issue fetch requests as if it were a replica broker for debugging purposes.
        ///  MaxWaitTime => int32 -- The max wait time is the maximum amount of time in milliseconds to block waiting if insufficient data is available 
        ///                          at the time the request is issued.
        ///  MinBytes => int32    -- This is the minimum number of bytes of messages that must be available to give a response. If the client sets this 
        ///                          to 0 the server will always respond immediately, however if there is no new data since their last request they will 
        ///                          just get back empty message sets. If this is set to 1, the server will respond as soon as at least one partition has 
        ///                          at least 1 byte of data or the specified timeout occurs. By setting higher values in combination with the timeout the 
        ///                          consumer can tune for throughput and trade a little additional latency for reading only large chunks of data (e.g. 
        ///                          setting MaxWaitTime to 100 ms and setting MinBytes to 64k would allow the server to wait up to 100ms to try to accumulate 
        ///                          64k of data before responding).
        ///  TopicName => string  -- The name of the topic.
        ///  Partition => int32   -- The id of the partition the fetch is for.
        ///  FetchOffset => int64 -- The offset to begin this fetch from.
        ///  MaxBytes => int32    -- The maximum bytes to include in the message set for this partition. This helps bound the size of the response.
        /// 
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-FetchAPI
        /// </summary>
        [Test]
        public void FetchRequest(
            [Values(0, 1, 2)] short version,
            [Values(0, 100)] int maxWaitMilliseconds, 
            [Values(0, 64000)] int minBytes, 
            [Values("test", "a really long name, with spaces and punctuation!")] string topic, 
            [Values(1, 10)] int topicsPerRequest, 
            [Values(1, 5)] int totalPartitions, 
            [Values(25600000)] int maxBytes)
        {
            var fetches = new List<FetchRequest.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                fetches.Add(new FetchRequest.Topic(topic + t, t % totalPartitions, _randomizer.Next(0, int.MaxValue), maxBytes));
            }
            var request = new FetchRequest(fetches, TimeSpan.FromMilliseconds(maxWaitMilliseconds), minBytes);
            request.AssertCanEncodeDecodeRequest(version);
        }

        /// <summary>
        /// FetchResponse => *ThrottleTime [TopicName [Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet]]
        ///  *ThrottleTime is only version 1 (0.9.0) and above
        ///  ThrottleTime => int32        -- Duration in milliseconds for which the request was throttled due to quota violation. (Zero if the request did not 
        ///                                  violate any quota.)
        ///  TopicName => string          -- The topic this response entry corresponds to.
        ///  Partition => int32           -- The partition this response entry corresponds to.
        ///  ErrorCode => int16           -- The error from this partition, if any. Errors are given on a per-partition basis because a given partition may 
        ///                                  be unavailable or maintained on a different host, while others may have successfully accepted the produce request.
        ///  HighwaterMarkOffset => int64 -- The offset at the end of the log for this partition. This can be used by the client to determine how many messages 
        ///                                  behind the end of the log they are.
        ///  MessageSetSize => int32      -- The size in bytes of the message set for this partition
        /// 
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-FetchResponse
        /// </summary>
        [Test]
        public void FetchResponse(
            [Values(0, 1, 2)] short version,
            [Values(0, 1234)] int throttleTime,
            [Values("test", "a really long name, with spaces and punctuation!")] string topicName, 
            [Values(1, 10)] int topicsPerRequest, 
            [Values(1, 5)] int totalPartitions, 
            [Values(
                ErrorResponseCode.None,
                ErrorResponseCode.OffsetOutOfRange
            )] ErrorResponseCode errorCode, 
            [Values(3)] int messagesPerSet
            )
        {
            var topics = new List<FetchResponse.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                var partitionId = t % totalPartitions;
                var messages = GenerateMessages(messagesPerSet, (byte) (version >= 2 ? 1 : 0), partitionId);
                topics.Add(new FetchResponse.Topic(topicName + t, partitionId, _randomizer.Next(), errorCode, messages));
            }
            var response = new FetchResponse(topics, version >= 1 ? TimeSpan.FromMilliseconds(throttleTime) : (TimeSpan?)null);

            response.AssertCanEncodeDecodeResponse(version);
        }

        /// <summary>
        /// OffsetRequest => ReplicaId [TopicName [Partition Timestamp MaxNumberOfOffsets]]
        ///  ReplicaId => int32   -- The replica id indicates the node id of the replica initiating this request. Normal client consumers should always 
        ///                          specify this as -1 as they have no node id. Other brokers set this to be their own node id. The value -2 is accepted 
        ///                          to allow a non-broker to issue fetch requests as if it were a replica broker for debugging purposes.
        ///  TopicName => string  -- The name of the topic.
        ///  Partition => int32   -- The id of the partition the fetch is for.
        ///  Timestamp => int64        -- Used to ask for all messages before a certain time (ms). There are two special values. Specify -1 to receive the 
        ///                          latest offset (i.e. the offset of the next coming message) and -2 to receive the earliest available offset. Note 
        ///                          that because offsets are pulled in descending order, asking for the earliest offset will always return you a single element.
        ///  MaxNumberOfOffsets => int32 
        /// 
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetAPI(AKAListOffset)
        /// </summary>
        [Test]
        public void OffsetsRequest(
            [Values("test", "a really long name, with spaces and punctuation!")] string topic, 
            [Values(1, 10)] int topicsPerRequest, 
            [Values(1, 5)] int totalPartitions, 
            [Values(-2, -1, 123456, 10000000)] long time,
            [Values(1, 10)] int maxOffsets)
        {
            var offsets = new List<OffsetRequest.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                var offset = new OffsetRequest.Topic(topic + t, t % totalPartitions, time, maxOffsets);
                offsets.Add(offset);
            }
            var request = new OffsetRequest(offsets);

            request.AssertCanEncodeDecodeRequest(0);
        }

        /// <summary>
        /// OffsetResponse => [TopicName [PartitionOffsets]]
        ///  PartitionOffsets => Partition ErrorCode [Offset]
        ///  TopicName => string  -- The name of the topic.
        ///  Partition => int32   -- The id of the partition the fetch is for.
        ///  ErrorCode => int16   -- The error from this partition, if any. Errors are given on a per-partition basis because a given partition may 
        ///                          be unavailable or maintained on a different host, while others may have successfully accepted the produce request.
        ///  Offset => int64
        /// 
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetAPI(AKAListOffset)
        /// </summary>
        [Test]
        public void OffsetsResponse(
            [Values("test", "a really long name, with spaces and punctuation!")] string topicName, 
            [Values(1, 10)] int topicsPerRequest, 
            [Values(5)] int totalPartitions, 
            [Values(
                ErrorResponseCode.UnknownTopicOrPartition,
                ErrorResponseCode.NotLeaderForPartition,
                ErrorResponseCode.Unknown
            )] ErrorResponseCode errorCode, 
            [Values(1, 5)] int offsetsPerPartition)
        {
            var topics = new List<OffsetResponse.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                var partitionId = t % totalPartitions;
                for (var o = 0; o < offsetsPerPartition; o++) {
                    topics.Add(new OffsetResponse.Topic(topicName + t, partitionId, errorCode, (long)_randomizer.Next()));
                }
            }
            var response = new OffsetResponse(topics);

            response.AssertCanEncodeDecodeResponse(0);
        }

        /// <summary>
        /// TopicMetadataRequest => [TopicName]
        ///  TopicName => string  -- The topics to produce metadata for. If no topics are specified fetch metadata for all topics.
        ///
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-MetadataAPI
        /// </summary>
        [Test]
        public void MetadataRequest(
            [Values("test", "a really long name, with spaces and punctuation!")] string topic,
            [Values(0, 1, 10)] int topicsPerRequest)
        {
            var topics = new List<string>();
            for (var t = 0; t < topicsPerRequest; t++) {
                topics.Add(topic + t);
            }
            var request = new MetadataRequest(topics);

            request.AssertCanEncodeDecodeRequest(0);
        }

        [Test]
        public void MetadataResponse(
            [Values(0, 1)] short version,
            [Values(1, 15)] int brokersPerRequest,
            [Values("test", "a really long name, with spaces and punctuation!")] string topicName,
            [Values(1, 10)] int topicsPerRequest,
            [Values(1, 5)] int partitionsPerTopic,
            [Values(
                 ErrorResponseCode.None,
                 ErrorResponseCode.UnknownTopicOrPartition
             )] ErrorResponseCode errorCode)
        {
            var brokers = new List<Broker>();
            for (var b = 0; b < brokersPerRequest; b++) {
                string rack = null;
                if (version >= 1) {
                    rack = "Rack" + b;
                }
                brokers.Add(new Broker(b, "broker-" + b, 9092 + b, rack));
            }
            var topics = new List<MetadataResponse.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                var partitions = new List<MetadataResponse.Partition>();
                for (var partitionId = 0; partitionId < partitionsPerTopic; partitionId++) {
                    var leader = _randomizer.Next(0, brokersPerRequest - 1);
                    var replica = 0;
                    var replicas = _randomizer.Next(0, brokersPerRequest - 1).Repeat(() => replica++);
                    var isr = 0;
                    var isrs = _randomizer.Next(0, replica).Repeat(() => isr++);
                    partitions.Add(new MetadataResponse.Partition(partitionId, leader, errorCode, replicas, isrs));
                }
                topics.Add(new MetadataResponse.Topic(topicName + t, errorCode, partitions, version >= 1 ? topicsPerRequest%2 == 0 : (bool?)null));
            }
            var response = new MetadataResponse(brokers, topics, version >= 1 ? brokersPerRequest : (int?)null);

            response.AssertCanEncodeDecodeResponse(version);
        }

        /// <summary>
        /// OffsetCommitRequest => ConsumerGroup *ConsumerGroupGenerationId *MemberId *RetentionTime [TopicName [Partition Offset *TimeStamp Metadata]]
        /// *ConsumerGroupGenerationId, MemberId is only version 1 (0.8.2) and above
        /// *TimeStamp is only version 1 (0.8.2)
        /// *RetentionTime is only version 2 (0.9.0) and above
        ///  ConsumerGroupId => string          -- The consumer group id.
        ///  ConsumerGroupGenerationId => int32 -- The generation of the consumer group.
        ///  MemberId => string                 -- The consumer id assigned by the group coordinator.
        ///  RetentionTime => int64             -- Timestamp period in ms to retain the offset.
        ///  TopicName => string                -- The topic to commit.
        ///  Partition => int32                 -- The partition id.
        ///  Offset => int64                    -- message offset to be committed.
        ///  Timestamp => int64                 -- Commit timestamp.
        ///  Metadata => string                 -- Any associated metadata the client wants to keep
        ///
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
        /// </summary>
        [Test]
        public void OffsetCommitRequest(
            [Values(0, 1, 2)] short version,
            [Values("group1", "group2")] string groupId,
            [Values(0, 5)] int generation,
            [Values(-1, 20000)] int retentionTime,
            [Values("test", "a really long name, with spaces and punctuation!")] string topic,
            [Values(1, 10)] int topicsPerRequest,
            [Values(5)] int maxPartitions,
            [Values(10)] int maxOffsets,
            [Values(null, "something useful for the client")] string metadata)
        {
            var offsetCommits = new List<OffsetCommitRequest.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                offsetCommits.Add(new OffsetCommitRequest.Topic(
                                      topic + t,
                                      t%maxPartitions,
                                      _randomizer.Next(0, int.MaxValue),
                                      metadata,
                                      version == 1 ? retentionTime : (long?)null));
            }
            var request = new OffsetCommitRequest(
                groupId,
                offsetCommits,
                version >= 1 ? "member" + generation : null,
                version >= 1 ? generation : 0,
                version >= 2 && retentionTime >= 0 ? (TimeSpan?) TimeSpan.FromMilliseconds(retentionTime) : null);

            request.AssertCanEncodeDecodeRequest(version);
        }

        /// <summary>
        /// OffsetCommitResponse => [TopicName [Partition ErrorCode]]]
        ///  TopicName => string -- The name of the topic.
        ///  Partition => int32  -- The id of the partition.
        ///  ErrorCode => int16  -- The error code for the partition, if any.
        ///
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
        /// </summary>
        [Test]
        public void OffsetCommitResponse(
            [Values("test", "a really long name, with spaces and punctuation!")] string topicName,
            [Values(1, 10)] int topicsPerRequest,
            [Values(1, 5)] int partitionsPerTopic,
            [Values(
                 ErrorResponseCode.None,
                 ErrorResponseCode.OffsetMetadataTooLarge
             )] ErrorResponseCode errorCode)
        {
            var topics = new List<TopicResponse>();
            for (var t = 0; t < topicsPerRequest; t++) {
                for (var partitionId = 0; partitionId < partitionsPerTopic; partitionId++) {
                    topics.Add(new TopicResponse(topicName + t, partitionId, errorCode));
                }
            }
            var response = new OffsetCommitResponse(topics);

            response.AssertCanEncodeDecodeResponse(0);
        }

        /// <summary>
        /// OffsetFetchRequest => ConsumerGroup [TopicName [Partition]]
        ///  ConsumerGroup => string -- The consumer group id.
        ///  TopicName => string     -- The topic to commit.
        ///  Partition => int32      -- The partition id.
        ///
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
        /// </summary>
        [Test]
        public void OffsetFetchRequest(
            [Values("group1", "group2")] string groupId,
            [Values("test", "a really long name, with spaces and punctuation!")] string topic,
            [Values(1, 10)] int topicsPerRequest,
            [Values(5)] int maxPartitions)
        {
            var topics = new List<TopicPartition>();
            for (var t = 0; t < topicsPerRequest; t++) {
                topics.Add(new TopicPartition(topic + t, t % maxPartitions));
            }
            var request = new OffsetFetchRequest(groupId, topics);

            request.AssertCanEncodeDecodeRequest(0);
        }

        /// <summary>
        /// OffsetFetchResponse => [TopicName [Partition Offset Metadata ErrorCode]]
        ///  TopicName => string -- The name of the topic.
        ///  Partition => int32  -- The id of the partition.
        ///  Offset => int64     -- The offset, or -1 if none exists.
        ///  Metadata => string  -- The metadata associated with the topic and partition.
        ///  ErrorCode => int16  -- The error code for the partition, if any.
        ///
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
        /// </summary>
        [Test]
        public void OffsetFetchResponse(
            [Values("test", "a really long name, with spaces and punctuation!")] string topicName,
            [Values(1, 10)] int topicsPerRequest,
            [Values(1, 5)] int partitionsPerTopic,
            [Values(
                 ErrorResponseCode.None,
                 ErrorResponseCode.UnknownTopicOrPartition,
                 ErrorResponseCode.GroupLoadInProgress,
                 ErrorResponseCode.NotCoordinatorForGroup,
                 ErrorResponseCode.IllegalGeneration,
                 ErrorResponseCode.UnknownMemberId,
                 ErrorResponseCode.TopicAuthorizationFailed,
                 ErrorResponseCode.GroupAuthorizationFailed
             )] ErrorResponseCode errorCode)
        {
            var topics = new List<OffsetFetchResponse.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                for (var partitionId = 0; partitionId < partitionsPerTopic; partitionId++) {
                    var offset = (long)_randomizer.Next(int.MinValue, int.MaxValue);
                    topics.Add(new OffsetFetchResponse.Topic(topicName + t, partitionId, errorCode, offset, offset >= 0 ? topicName : string.Empty));
                }
            }
            var response = new OffsetFetchResponse(topics);

            response.AssertCanEncodeDecodeResponse(0);
        }

        /// <summary>
        /// GroupCoordinatorRequest => GroupId
        ///  GroupId => string -- The consumer group id.
        ///
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
        /// </summary>
        [Test]
        public void GroupCoordinatorRequest([Values("group1", "group2")] string groupId)
        {
            var request = new GroupCoordinatorRequest(groupId);
            request.AssertCanEncodeDecodeRequest(0);
        }

        /// <summary>
        /// GroupCoordinatorResponse => ErrorCode CoordinatorId CoordinatorHost CoordinatorPort
        ///  ErrorCode => int16        -- The error code.
        ///  CoordinatorId => int32    -- The broker id.
        ///  CoordinatorHost => string -- The hostname of the broker.
        ///  CoordinatorPort => int32  -- The port on which the broker accepts requests.
        ///
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
        /// </summary>
        [Test]
        public void GroupCoordinatorResponse(
            [Values(
                 ErrorResponseCode.None,
                 ErrorResponseCode.GroupCoordinatorNotAvailable,
                 ErrorResponseCode.GroupAuthorizationFailed
             )] ErrorResponseCode errorCode,
            [Values(0, 1)] int coordinatorId
            )
        {
            var response = new GroupCoordinatorResponse(errorCode, coordinatorId, "broker-" + coordinatorId, 9092 + coordinatorId);

            response.AssertCanEncodeDecodeResponse(0);
        }

        /// <summary>
        /// ApiVersions => 
        ///
        /// From http://kafka.apache.org/protocol.html#protocol_messages
        /// </summary>
        [Test]
        public void ApiVersionsRequest()
        {
            var request = new ApiVersionsRequest();
            request.AssertCanEncodeDecodeRequest(0);
        }

        /// <summary>
        /// ApiVersionsResponse => ErrorCode [ApiKey MinVersion MaxVersion]
        ///  ErrorCode => int16  -- The error code.
        ///  ApiKey => int16     -- The Api Key.
        ///  MinVersion => int16 -- The minimum supported version.
        ///  MaxVersion => int16 -- The maximum supported version.
        ///
        /// From http://kafka.apache.org/protocol.html#protocol_messages
        /// </summary>
        [Test]
        public void ApiVersionsResponse(
            [Values(
                 ErrorResponseCode.None,
                 ErrorResponseCode.BrokerNotAvailable
             )] ErrorResponseCode errorCode
            )
        {
            var supported = new List<ApiVersionsResponse.VersionSupport>();
            for (short apiKey = 0; apiKey <= 18; apiKey++) {
                supported.Add(new ApiVersionsResponse.VersionSupport((ApiKeyRequestType)apiKey, 0, (short)_randomizer.Next(0, 2)));
            }
            var response = new ApiVersionsResponse(errorCode, supported);

            response.AssertCanEncodeDecodeResponse(0);
        }

        [Test]
        public void JoinGroupRequest(
            [Values("test", "a groupId")] string groupId, 
            [Values(1, 20000)] int sessionTimeout,
            [Values("", "an existing member")] string memberId, 
            [Values("consumer", "other")] string protocolType, 
            [Values(1, 10)] int protocolsPerRequest)
        {
            var protocols = new List<JoinGroupRequest.GroupProtocol>();
            for (var p = 0; p < protocolsPerRequest; p++) {
                var bytes = new byte[protocolsPerRequest*100];
                _randomizer.NextBytes(bytes);
                protocols.Add(new JoinGroupRequest.GroupProtocol(protocolType + p, new ByteMember(bytes)));
            }
            var request = new JoinGroupRequest(groupId, TimeSpan.FromMilliseconds(sessionTimeout), memberId, protocolType, protocols);

            request.AssertCanEncodeDecodeRequest(0);
        }

        [Test]
        public void JoinGroupResponse(
            [Values(
                 ErrorResponseCode.None,
                 ErrorResponseCode.OffsetMetadataTooLarge
             )] ErrorResponseCode errorCode,
            [Values(0, 1, 20000)] int generationId,
            [Values("consumer", "other")] string protocol, 
            [Values("test", "a groupId")] string leaderId, 
            [Values("", "an existing member")] string memberId, 
            [Values(1, 10)] int memberCount)
        {
            var members = new List<JoinGroupResponse.Member>();
            for (var m = 0; m < memberCount; m++) {
                var bytes = new byte[memberCount*100];
                _randomizer.NextBytes(bytes);
                members.Add(new JoinGroupResponse.Member(memberId + m, new ByteMember(bytes)));
            }
            var response = new JoinGroupResponse(errorCode, generationId, protocol, leaderId, memberId, members);

            response.AssertCanEncodeDecodeResponse(0);
        }

        [Test]
        public void JoinConsumerGroupRequest(
            [Values("test", "a groupId")] string groupId, 
            [Values(1, 20000)] int sessionTimeout,
            [Values("", "an existing member")] string memberId, 
            [Values("consumer")] string protocolType, 
            [Values(1, 10)] int protocolsPerRequest)
        {
            var encoder = new ConsumerEncoder();
            var protocols = new List<JoinGroupRequest.GroupProtocol>();
            for (var p = 0; p < protocolsPerRequest; p++) {
                var userData = new byte[protocolsPerRequest*100];
                _randomizer.NextBytes(userData);
                var metadata = new ConsumerProtocolMetadata(0, new []{ groupId, memberId, protocolType }, userData);
                protocols.Add(new JoinGroupRequest.GroupProtocol(protocolType + p, metadata));
            }
            var request = new JoinGroupRequest(groupId, TimeSpan.FromMilliseconds(sessionTimeout), memberId, protocolType, protocols);

            request.AssertCanEncodeDecodeRequest(0, encoder);
        }

        [Test]
        public void JoinConsumerGroupResponse(
            [Values(
                 ErrorResponseCode.None,
                 ErrorResponseCode.OffsetMetadataTooLarge
             )] ErrorResponseCode errorCode,
            [Values(0, 1, 20000)] int generationId,
            [Values("consumer")] string protocol, 
            [Values("test", "a groupId")] string leaderId, 
            [Values("", "an existing member")] string memberId, 
            [Values(1, 10)] int memberCount)
        {
            var encoder = new ConsumerEncoder();
            var members = new List<JoinGroupResponse.Member>();
            for (var m = 0; m < memberCount; m++) {
                var userData = new byte[memberCount*100];
                _randomizer.NextBytes(userData);
                var metadata = new ConsumerProtocolMetadata(0, new []{ protocol, memberId, leaderId }, userData);
                members.Add(new JoinGroupResponse.Member(memberId + m, metadata));
            }
            var response = new JoinGroupResponse(errorCode, generationId, protocol, leaderId, memberId, members);

            response.AssertCanEncodeDecodeResponse(0, encoder);
        }

        [Test]
        public void HeartbeatRequest(
            [Values("test", "a groupId")] string groupId, 
            [Values(0, 1, 20000)] int generationId,
            [Values("", "an existing member")] string memberId)
        {
            var request = new HeartbeatRequest(groupId, generationId, memberId);

            request.AssertCanEncodeDecodeRequest(0);
        }

        [Test]
        public void HeartbeatResponse(
            [Values(
                 ErrorResponseCode.None,
                 ErrorResponseCode.OffsetMetadataTooLarge
             )] ErrorResponseCode errorCode)
        {
            var response = new HeartbeatResponse(errorCode);

            response.AssertCanEncodeDecodeResponse(0);
        }

        [Test]
        public void LeaveGroupRequest(
            [Values("test", "a groupId")] string groupId, 
            [Values("", "an existing member")] string memberId)
        {
            var request = new LeaveGroupRequest(groupId, memberId);

            request.AssertCanEncodeDecodeRequest(0);
        }

        [Test]
        public void LeaveGroupResponse(
            [Values(
                 ErrorResponseCode.None,
                 ErrorResponseCode.OffsetMetadataTooLarge
             )] ErrorResponseCode errorCode)
        {
            var response = new LeaveGroupResponse(errorCode);

            response.AssertCanEncodeDecodeResponse(0);
        }

        [Test]
        public void SyncGroupRequest(
            [Values("test", "a groupId")] string groupId, 
            [Values(0, 1, 20000)] int generationId,
            [Values("", "an existing member")] string memberId, 
            [Values("consumer", "other")] string protocolType, 
            [Values(1, 10)] int assignmentsPerRequest)
        {
            var assignments = new List<SyncGroupRequest.GroupAssignment>();
            for (var a = 0; a < assignmentsPerRequest; a++) {
                var bytes = new byte[assignmentsPerRequest*100];
                _randomizer.NextBytes(bytes);
                assignments.Add(new SyncGroupRequest.GroupAssignment(protocolType + a, new ByteMember(bytes)));
            }
            var request = new SyncGroupRequest(groupId, generationId, memberId, assignments);

            request.AssertCanEncodeDecodeRequest(0);
        }

        [Test]
        public void SyncGroupResponse(
            [Values(
                 ErrorResponseCode.None,
                 ErrorResponseCode.OffsetMetadataTooLarge
             )] ErrorResponseCode errorCode)
        {
            var bytes = new byte[1000];
            _randomizer.NextBytes(bytes);
            var response = new SyncGroupResponse(errorCode, new ByteMember(bytes));

            response.AssertCanEncodeDecodeResponse(0);
        }

        [Test]
        public void SyncConsumerGroupRequest(
            [Values("test", "a groupId")] string groupId, 
            [Values(0, 1, 20000)] int generationId,
            [Values("", "an existing member")] string memberId, 
            [Values("consumer")] string protocolType, 
            [Values(1, 10)] int assignmentsPerRequest)
        {
            var encoder = new ConsumerEncoder();
            var assignments = new List<SyncGroupRequest.GroupAssignment>();
            for (var a = 0; a < assignmentsPerRequest; a++) {
                var topics = new List<TopicPartition>();
                for (var t = 0; t < assignmentsPerRequest; t++) {
                    topics.Add(new TopicPartition(groupId + t, t));
                }
                var assignment = new ConsumerMemberAssignment(0, topics);
                assignments.Add(new SyncGroupRequest.GroupAssignment(protocolType + a, assignment));
            }
            var request = new SyncGroupRequest(groupId, generationId, memberId, assignments);

            request.AssertCanEncodeDecodeRequest(0, encoder);
        }

        [Test]
        public void SyncConsumerGroupResponse(
            [Values(
                 ErrorResponseCode.None,
                 ErrorResponseCode.OffsetMetadataTooLarge
             )] ErrorResponseCode errorCode,
            [Values(1, 10)] int memberCount)
        {
            var encoder = new ConsumerEncoder();
            var topics = new List<TopicPartition>();
            for (var t = 0; t < memberCount; t++) {
                topics.Add(new TopicPartition("topic foo" + t, t));
            }
            var assignment = new ConsumerMemberAssignment(0, topics);
            var response = new SyncGroupResponse(errorCode, assignment);

            response.AssertCanEncodeDecodeResponse(0, encoder);
        }

        [Test]
        public void DescribeGroupsRequest(
            [Values("test", "a groupId")] string groupId, 
            [Range(1, 10)] int count)
        {
            var groups = new string[count];
            for (var g = 0; g < count; g++) {
                groups[g] = groupId + g;
            }
            var request = new DescribeGroupsRequest(groups);

            request.AssertCanEncodeDecodeRequest(0);
        }

        [Test]
        public void DescribeGroupsResponse(
            [Values(
                 ErrorResponseCode.None,
                 ErrorResponseCode.OffsetMetadataTooLarge
             )] ErrorResponseCode errorCode,
            [Values("test", "a groupId")] string groupId, 
            [Range(2, 3)] int count,
            [Values(KafkaClient.Protocol.DescribeGroupsResponse.Group.States.Stable, KafkaClient.Protocol.DescribeGroupsResponse.Group.States.Dead)] string state, 
            [Values("consumer", "unknown")] string protocolType,
            [Values("good", "bad", "ugly")] string protocol)
        {
            var groups = new DescribeGroupsResponse.Group[count];
            for (var g = 0; g < count; g++) {
                var members = new List<DescribeGroupsResponse.Member>();
                for (var m = 0; m < count; m++) {
                    var metadata = new byte[count*100];
                    var assignment = new byte[count*10];
                    _randomizer.NextBytes(metadata);
                    _randomizer.NextBytes(assignment);

                    members.Add(new DescribeGroupsResponse.Member("member" + m, "client" + m, "host-" + m, new ByteMember(metadata), new ByteMember(assignment)));
                }
                groups[g] = new DescribeGroupsResponse.Group(errorCode, groupId + g, state, protocolType, protocol, members);
            }
            var response = new DescribeGroupsResponse(groups);

            response.AssertCanEncodeDecodeResponse(0);
        }

        [Test]
        public void DescribeConsumerGroupsResponse(
            [Values(
                 ErrorResponseCode.None,
                 ErrorResponseCode.OffsetMetadataTooLarge
             )] ErrorResponseCode errorCode,
            [Values("test", "a groupId")] string groupId, 
            [Range(2, 3)] int count,
            [Values(KafkaClient.Protocol.DescribeGroupsResponse.Group.States.Stable, KafkaClient.Protocol.DescribeGroupsResponse.Group.States.AwaitingSync)] string state, 
            [Values("consumer")] string protocolType,
            [Values("good", "bad", "ugly")] string protocol)
        {
            var encoder = new ConsumerEncoder();
            var groups = new DescribeGroupsResponse.Group[count];
            for (var g = 0; g < count; g++) {
                var members = new List<DescribeGroupsResponse.Member>();
                for (var m = 0; m < count; m++) {
                    var memberId = "member" + m;
                    var userData = new byte[count*100];
                    _randomizer.NextBytes(userData);
                    var metadata = new ConsumerProtocolMetadata(0, new []{ protocol, memberId, memberId }, userData);

                    var topics = new List<TopicPartition>();
                    for (var t = 0; t < count; t++) {
                        topics.Add(new TopicPartition("topic foo" + t, t));
                    }
                    var assignment = new ConsumerMemberAssignment(0, topics);

                    members.Add(new DescribeGroupsResponse.Member(memberId, "client" + m, "host-" + m, metadata, assignment));
                }
                groups[g] = new DescribeGroupsResponse.Group(errorCode, groupId + g, state, protocolType, protocol, members);
            }
            var response = new DescribeGroupsResponse(groups);

            response.AssertCanEncodeDecodeResponse(0, encoder);
        }

        [Test]
        public void ListGroupsRequest()
        {
            var request = new ListGroupsRequest();
            request.AssertCanEncodeDecodeRequest(0);
        }

        [Test]
        public void ListGroupsResponse(
            [Values(
                 ErrorResponseCode.None,
                 ErrorResponseCode.OffsetMetadataTooLarge
             )] ErrorResponseCode errorCode,
            [Values("test", "a groupId")] string groupId, 
            [Range(2, 3)] int count,
            [Values("consumer")] string protocolType)
        {
            var groups = new ListGroupsResponse.Group[count];
            for (var g = 0; g < count; g++) {
                groups[g] = new ListGroupsResponse.Group(groupId + g, protocolType);
            }
            var response = new ListGroupsResponse(errorCode, groups);

            response.AssertCanEncodeDecodeResponse(0);
        }

        [Test]
        public void SaslHandshakeRequest(
            [Values("EXTERNAL", "ANONYMOUS", "PLAIN", "OTP", "SKEY", "CRAM-MD5", "DIGEST-MD5", "SCRAM", "NTLM", "GSSAPI", "OAUTHBEARER")] string mechanism)
        {
            var request = new SaslHandshakeRequest(mechanism);

            request.AssertCanEncodeDecodeRequest(0);
        }

        [Test]
        public void SaslHandshakeResponse(
            [Values(
                 ErrorResponseCode.None,
                 ErrorResponseCode.OffsetMetadataTooLarge
             )] ErrorResponseCode errorCode,
            [Range(1, 11)] int count)
        {
            var mechanisms = new[] { "EXTERNAL", "ANONYMOUS", "PLAIN", "OTP", "SKEY", "CRAM-MD5", "DIGEST-MD5", "SCRAM", "NTLM", "GSSAPI", "OAUTHBEARER" };
            var response = new SaslHandshakeResponse(errorCode, mechanisms.Take(count));

            response.AssertCanEncodeDecodeResponse(0);
        }


        private IEnumerable<Message> GenerateMessages(int count, byte version, int partition = 0)
        {
            var messages = new List<Message>();
            for (var m = 0; m < count; m++) {
                var key = m > 0 ? new byte[8] : null;
                var value = new byte[8*(m + 1)];
                if (key != null) {
                    _randomizer.NextBytes(key);
                }
                _randomizer.NextBytes(value);

                messages.Add(new Message(value, 0, partitionId: partition, version: version, key: key, timestamp: version > 0 ? DateTime.UtcNow : (DateTime?)null));
            }
            return messages;
        }
    }
}