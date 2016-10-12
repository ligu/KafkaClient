using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using KafkaClient.Common;
using KafkaClient.Protocol;

namespace KafkaClient.Tests.Protocol
{
    [SuppressMessage("ReSharper", "UnusedParameter.Local")]
    public static class KafkaDecoder
    {
        public static IRequestContext DecodeHeader(byte[] data)
        {
            IRequestContext context;
            using (ReadHeader(data, out context)) {
                return context;
            }            
        }

        public static T Decode<T>(byte[] data, IRequestContext context = null) where T : class, IRequest
        {
            var protocolType = context?.ProtocolType;
            var encoders = context?.Encoders;
            using (ReadHeader(data, out context)) { }

            return Decode<T>(new RequestContext(context.CorrelationId, context.ApiVersion, context.ClientId, encoders, protocolType), data);
        }

        public static T Decode<T>(IRequestContext context, byte[] data) where T : class, IRequest
        {
            if (typeof(T) == typeof(ProduceRequest)) return (T)ProduceRequest(context, data);
            if (typeof(T) == typeof(FetchRequest)) return (T)FetchRequest(context, data);
            if (typeof(T) == typeof(OffsetRequest)) return (T)OffsetRequest(context, data);
            if (typeof(T) == typeof(MetadataRequest)) return (T)MetadataRequest(context, data);
            if (typeof(T) == typeof(StopReplicaRequest)) return (T)StopReplicaRequest(context, data);
            if (typeof(T) == typeof(OffsetCommitRequest)) return (T)OffsetCommitRequest(context, data);
            if (typeof(T) == typeof(OffsetFetchRequest)) return (T)OffsetFetchRequest(context, data);
            if (typeof(T) == typeof(GroupCoordinatorRequest)) return (T)GroupCoordinatorRequest(context, data);
            if (typeof(T) == typeof(JoinGroupRequest)) return (T)JoinGroupRequest(context, data);
            if (typeof(T) == typeof(HeartbeatRequest)) return (T)HeartbeatRequest(context, data);
            if (typeof(T) == typeof(LeaveGroupRequest)) return (T)LeaveGroupRequest(context, data);
            if (typeof(T) == typeof(SyncGroupRequest)) return (T)SyncGroupRequest(context, data);
            if (typeof(T) == typeof(DescribeGroupsRequest)) return (T)DescribeGroupsRequest(context, data);
            if (typeof(T) == typeof(ListGroupsRequest)) return (T)ListGroupsRequest(context, data);
            if (typeof(T) == typeof(SaslHandshakeRequest)) return (T)SaslHandshakeRequest(context, data);
            if (typeof(T) == typeof(ApiVersionsRequest)) return (T)ApiVersionsRequest(context, data);
            return default(T);
        }

        public static byte[] EncodeResponseBytes<T>(IRequestContext context, T response) where T : IResponse
        {
            using (var writer = new KafkaWriter()) {
                // From http://kafka.apache.org/protocol.html#protocol_messages
                // 
                // Response Header => correlation_id 
                //  correlation_id => INT32  -- The user-supplied value passed in with the request
                writer.Write(context.CorrelationId);

                // ReSharper disable once UnusedVariable
                var isEncoded = 
                   TryEncodeResponse(writer, context, response as ProduceResponse)
                || TryEncodeResponse(writer, context, response as FetchResponse)
                || TryEncodeResponse(writer, context, response as OffsetResponse)
                || TryEncodeResponse(writer, context, response as MetadataResponse)
                || TryEncodeResponse(writer, context, response as StopReplicaResponse)
                || TryEncodeResponse(writer, context, response as OffsetCommitResponse)
                || TryEncodeResponse(writer, context, response as OffsetFetchResponse)
                || TryEncodeResponse(writer, context, response as GroupCoordinatorResponse)
                || TryEncodeResponse(writer, context, response as JoinGroupResponse)
                || TryEncodeResponse(writer, context, response as HeartbeatResponse)
                || TryEncodeResponse(writer, context, response as LeaveGroupResponse)
                || TryEncodeResponse(writer, context, response as SyncGroupResponse)
                || TryEncodeResponse(writer, context, response as DescribeGroupsResponse)
                || TryEncodeResponse(writer, context, response as ListGroupsResponse)
                || TryEncodeResponse(writer, context, response as SaslHandshakeResponse)
                || TryEncodeResponse(writer, context, response as ApiVersionsResponse);

                return writer.ToBytes();
            }
        }

        #region Decode

        private static IRequest ProduceRequest(IRequestContext context, byte[] data)
        {
            using (var reader = ReadHeader(data)) {
                var acks = reader.ReadInt16();
                var timeout = reader.ReadInt32();

                var payloads = new List<Payload>();
                var payloadCount = reader.ReadInt32();
                for (var i = 0; i < payloadCount; i++) {
                    var topicName = reader.ReadString();

                    var partitionCount = reader.ReadInt32();
                    for (var j = 0; j < partitionCount; j++) {
                        var partitionId = reader.ReadInt32();
                        var messages = KafkaEncoder.DecodeMessageSet(reader.ReadBytes(), partitionId).ToList();

                        payloads.Add(new Payload(topicName, partitionId, messages));
                    }
                }
                return new ProduceRequest(payloads, TimeSpan.FromMilliseconds(timeout), acks);
            }
        }

        private static IRequest FetchRequest(IRequestContext context, byte[] data)
        {
            using (var reader = ReadHeader(data)) {
                // ReSharper disable once UnusedVariable
                var replicaId = reader.ReadInt32(); // expect -1
                var maxWaitTime = reader.ReadInt32();
                var minBytes = reader.ReadInt32();

                var fetches = new List<Fetch>();
                var payloadCount = reader.ReadInt32();
                for (var i = 0; i < payloadCount; i++) {
                    var topicName = reader.ReadString();

                    var partitionCount = reader.ReadInt32();
                    for (var j = 0; j < partitionCount; j++) {
                        var partitionId = reader.ReadInt32();
                        var offset = reader.ReadInt64();
                        var maxBytes = reader.ReadInt32();

                        fetches.Add(new Fetch(topicName, partitionId, offset, maxBytes));
                    }
                }
                return new FetchRequest(fetches, TimeSpan.FromMilliseconds(maxWaitTime), minBytes);
            }
        }

        private static IRequest OffsetRequest(IRequestContext context, byte[] data)
        {
            using (var reader = ReadHeader(data)) {
                // ReSharper disable once UnusedVariable
                var replicaId = reader.ReadInt32(); // expect -1

                var offsets = new List<Offset>();
                var offsetCount = reader.ReadInt32();
                for (var i = 0; i < offsetCount; i++) {
                    var topicName = reader.ReadString();

                    var partitionCount = reader.ReadInt32();
                    for (var j = 0; j < partitionCount; j++) {
                        var partitionId = reader.ReadInt32();
                        var time = reader.ReadInt64();
                        var maxOffsets = reader.ReadInt32();

                        offsets.Add(new Offset(topicName, partitionId, time, maxOffsets));
                    }
                }
                return new OffsetRequest(offsets);
            }
        }

        private static IRequest MetadataRequest(IRequestContext context, byte[] data)
        {
            using (var reader = ReadHeader(data)) {
                var topicNames = new string[reader.ReadInt32()];
                for (var t = 0; t < topicNames.Length; t++) {
                    topicNames[t] = reader.ReadString();
                }

                return new MetadataRequest(topicNames);
            }
        }

        private static IRequest StopReplicaRequest(IRequestContext context, byte[] payload)
        {
            using (var reader = ReadHeader(payload)) {
                var controllerId = reader.ReadInt32();
                var controllerEpoch = reader.ReadInt32();
                var shouldDelete = reader.ReadBoolean();

                var topics = new Topic[reader.ReadInt32()];
                for (var t = 0; t < topics.Length; t++) {
                    var topicName = reader.ReadString();
                    var partitionId = reader.ReadInt32();
                    topics[t] = new Topic(topicName, partitionId);
                }

                return new StopReplicaRequest(controllerId, controllerEpoch, topics, shouldDelete);
            }
        }
        
        private static IRequest OffsetCommitRequest(IRequestContext context, byte[] payload)
        {
            using (var reader = ReadHeader(payload)) {
                var consumerGroup = reader.ReadString();
                var generationId = 0;
                string memberId = null; 
                if (context.ApiVersion >= 1) {
                    generationId = reader.ReadInt32();
                    memberId = reader.ReadString();
                }
                TimeSpan? offsetRetention = null;
                if (context.ApiVersion >= 2) {
                    var retentionTime = reader.ReadInt64();
                    if (retentionTime >= 0) {
                        offsetRetention = TimeSpan.FromMilliseconds(retentionTime);
                    }
                }

                var offsetCommits = new List<OffsetCommit>();
                var count = reader.ReadInt32();
                for (var o = 0; o < count; o++) {
                    var topicName = reader.ReadString();

                    var partitionCount = reader.ReadInt32();
                    for (var p = 0; p < partitionCount; p++) {
                        var partitionId = reader.ReadInt32();
                        var offset = reader.ReadInt64();
                        long? timestamp = null;
                        if (context.ApiVersion == 1) {
                            timestamp = reader.ReadInt64();
                        }
                        var metadata = reader.ReadString();

                        offsetCommits.Add(new OffsetCommit(topicName, partitionId, offset, metadata, timestamp));
                    }
                }

                return new OffsetCommitRequest(consumerGroup, offsetCommits, memberId, generationId, offsetRetention);
            }
        }

        private static IRequest OffsetFetchRequest(IRequestContext context, byte[] payload)
        {
            using (var reader = ReadHeader(payload)) {
                var consumerGroup = reader.ReadString();

                var topics = new List<Topic>();
                var count = reader.ReadInt32();
                for (var t = 0; t < count; t++) {
                    var topicName = reader.ReadString();

                    var partitionCount = reader.ReadInt32();
                    for (var p = 0; p < partitionCount; p++) {
                        var partitionId = reader.ReadInt32();

                        topics.Add(new Topic(topicName, partitionId));
                    }
                }

                return new OffsetFetchRequest(consumerGroup, topics);
            }
        }
        
        private static IRequest GroupCoordinatorRequest(IRequestContext context, byte[] payload)
        {
            using (var reader = ReadHeader(payload)) {
                var groupId = reader.ReadString();

                return new GroupCoordinatorRequest(groupId);
            }
        }

        private static IRequest JoinGroupRequest(IRequestContext context, byte[] payload)
        {
            using (var reader = ReadHeader(payload)) {
                var groupId = reader.ReadString();
                var sessionTimeout = TimeSpan.FromMilliseconds(reader.ReadInt32());
                var memberId = reader.ReadString();
                var protocolType = reader.ReadString();
                var groupProtocols = new GroupProtocol[reader.ReadInt32()];

                var encoder = context.GetEncoder(protocolType);
                for (var g = 0; g < groupProtocols.Length; g++) {
                    var protocolName = reader.ReadString();
                    var metadata = encoder.DecodeMetadata(reader);
                    groupProtocols[g] = new GroupProtocol(protocolName, metadata);
                }

                return new JoinGroupRequest(groupId, sessionTimeout, memberId, protocolType, groupProtocols);
            }
        }

        private static IRequest HeartbeatRequest(IRequestContext context, byte[] payload)
        {
            using (var reader = ReadHeader(payload)) {
                var groupId = reader.ReadString();
                var generationId = reader.ReadInt32();
                var memberId = reader.ReadString();

                return new HeartbeatRequest(groupId, generationId, memberId);
            }
        }

        private static IRequest LeaveGroupRequest(IRequestContext context, byte[] payload)
        {
            using (var reader = ReadHeader(payload)) {
                var groupId = reader.ReadString();
                var memberId = reader.ReadString();

                return new LeaveGroupRequest(groupId, memberId);
            }
        }

        private static IRequest SyncGroupRequest(IRequestContext context, byte[] payload)
        {
            using (var reader = ReadHeader(payload)) {
                var groupId = reader.ReadString();
                var generationId = reader.ReadInt32();
                var memberId = reader.ReadString();

                var encoder = context.GetEncoder();
                var groupAssignments = new SyncGroupAssignment[reader.ReadInt32()];
                for (var a = 0; a < groupAssignments.Length; a++) {
                    var groupMemberId = reader.ReadString();
                    var assignment = encoder.DecodeAssignment(reader);

                    groupAssignments[a] = new SyncGroupAssignment(groupMemberId, assignment);
                }

                return new SyncGroupRequest(groupId, generationId, memberId, groupAssignments);
            }
        }

        private static IRequest DescribeGroupsRequest(IRequestContext context, byte[] payload)
        {
            using (var reader = ReadHeader(payload)) {
                var groupIds = new string[reader.ReadInt32()];
                for (var i = 0; i < groupIds.Length; i++) {
                    groupIds[i] = reader.ReadString();
                }

                return new DescribeGroupsRequest(groupIds);
            }
        }

        private static IRequest ListGroupsRequest(IRequestContext context, byte[] payload)
        {
            using (ReadHeader(payload)) {
                return new ListGroupsRequest();
            }
        }

        private static IRequest SaslHandshakeRequest(IRequestContext context, byte[] payload)
        {
            using (var reader = ReadHeader(payload)) {
                var mechanism = reader.ReadString();
                return new SaslHandshakeRequest(mechanism);
            }
        }

        private static IRequest ApiVersionsRequest(IRequestContext context, byte[] payload)
        {
            using (ReadHeader(payload)) {
                return new ApiVersionsRequest();
            }
        }
        
        private static IKafkaReader ReadHeader(byte[] data)
        {
            IRequestContext context;
            return ReadHeader(data, out context);
        }

        private static IKafkaReader ReadHeader(byte[] data, out IRequestContext context)
        {
            var reader = new BigEndianBinaryReader(data, 4);
            try {
                // ReSharper disable once UnusedVariable
                var apiKey = (ApiKeyRequestType) reader.ReadInt16();
                var version = reader.ReadInt16();
                var correlationId = reader.ReadInt32();
                var clientId = reader.ReadString();

                context = new RequestContext(correlationId, version, clientId);
            } catch {
                context = null;
                reader.Dispose();
                reader = null;
            }
            return reader;
        } 

        #endregion

        #region Encode

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, ProduceResponse response)
        {
            if (response == null) return false;

            var groupedTopics = response.Topics.GroupBy(t => t.TopicName).ToList();
            writer.Write(groupedTopics.Count);
            foreach (var topic in groupedTopics) {
                var partitions = topic.ToList();

                writer.Write(topic.Key)
                    .Write(partitions.Count); // partitionsPerTopic
                foreach (var partition in partitions) {
                    writer.Write(partition.PartitionId)
                        .Write(partition.ErrorCode)
                        .Write(partition.Offset);
                    if (context.ApiVersion >= 2) {
                        writer.Write(partition.Timestamp.ToUnixEpochMilliseconds() ?? -1L);
                    }
                }
            }
            if (context.ApiVersion >= 1) {
                writer.Write((int?)response.ThrottleTime?.TotalMilliseconds ?? 0);
            }
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, FetchResponse response)
        {
            if (response == null) return false;

            if (context.ApiVersion >= 1) {
                writer.Write((int?)response.ThrottleTime?.TotalMilliseconds ?? 0);
            }
            var groupedTopics = response.Topics.GroupBy(t => t.TopicName).ToList();
            writer.Write(groupedTopics.Count);
            foreach (var topic in groupedTopics) {
                var partitions = topic.ToList();

                writer.Write(topic.Key)
                    .Write(partitions.Count); // partitionsPerTopic
                foreach (var partition in partitions) {
                    writer.Write(partition.PartitionId)
                        .Write(partition.ErrorCode)
                        .Write(partition.HighWaterMark);

                    var messageSet = KafkaEncoder.EncodeMessageSet(partition.Messages);
                    writer.Write(messageSet, true);
                }
            }
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, OffsetResponse response)
        {
            if (response == null) return false;

            var groupedTopics = response.Topics.GroupBy(t => t.TopicName).ToList();
            writer.Write(groupedTopics.Count);
            foreach (var topic in groupedTopics) {
                var partitions = topic.ToList();

                writer.Write(topic.Key)
                    .Write(partitions.Count); // partitionsPerTopic
                foreach (var partition in partitions) {
                    writer.Write(partition.PartitionId)
                        .Write(partition.ErrorCode)
                        .Write(partition.Offsets.Count);
                    foreach (var offset in partition.Offsets) {
                        writer.Write(offset);
                    }
                }
            }
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, MetadataResponse response)
        {
            if (response == null) return false;

            writer.Write(response.Brokers.Count);
            foreach (var broker in response.Brokers) {
                writer.Write(broker.BrokerId)
                    .Write(broker.Host)
                    .Write(broker.Port);
            }

            var groupedTopics = response.Topics.GroupBy(t => new { t.TopicName, t.ErrorCode }).ToList();
            writer.Write(groupedTopics.Count);
            foreach (var topic in groupedTopics) {
                var partitions = topic.SelectMany(_ => _.Partitions).ToList();

                writer.Write(topic.Key.ErrorCode)
                    .Write(topic.Key.TopicName)
                    .Write(partitions.Count); // partitionsPerTopic
                foreach (var partition in partitions) {
                    writer.Write(partition.ErrorCode)
                        .Write(partition.PartitionId)
                        .Write(partition.LeaderId)
                        .Write(partition.Replicas.Count);
                    foreach (var replica in partition.Replicas) {
                        writer.Write(replica);
                    }
                    writer.Write(partition.Isrs.Count);
                    foreach (var isr in partition.Isrs) {
                        writer.Write(isr);
                    }
                }
            }
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, StopReplicaResponse response)
        {
            if (response == null) return false;

            writer.Write(response.ErrorCode)
                .Write(response.Topics.Count);
            foreach (var topic in response.Topics) {
                writer.Write(topic.TopicName)
                    .Write(topic.PartitionId)
                    .Write(topic.ErrorCode);
            }
            return true;
        }
        
        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, OffsetCommitResponse response)
        {
            if (response == null) return false;

            var groupedTopics = response.Topics.GroupBy(t => t.TopicName).ToList();
            writer.Write(groupedTopics.Count);
            foreach (var topic in groupedTopics) {
                var partitions = topic.ToList();
                writer.Write(topic.Key)
                    .Write(partitions.Count); // partitionsPerTopic
                foreach (var partition in partitions) {
                    writer.Write(partition.PartitionId)
                        .Write(partition.ErrorCode);
                }
            }
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, OffsetFetchResponse response)
        {
            if (response == null) return false;

            var groupedTopics = response.Topics.GroupBy(t => t.TopicName).ToList();
            writer.Write(groupedTopics.Count);
            foreach (var topic in groupedTopics) {
                var partitions = topic.ToList();
                writer.Write(topic.Key)
                    .Write(partitions.Count); // partitionsPerTopic
                foreach (var partition in partitions) {
                    writer.Write(partition.PartitionId)
                        .Write(partition.Offset)
                        .Write(partition.MetaData)
                        .Write(partition.ErrorCode);
                }
            }
            return true;
        }
        
        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, GroupCoordinatorResponse response)
        {
            if (response == null) return false;

            writer.Write(response.ErrorCode)
                .Write(response.BrokerId)
                .Write(response.Host)
                .Write(response.Port);
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, JoinGroupResponse response)
        {
            if (response == null) return false;

            writer.Write(response.ErrorCode)
                .Write(response.GenerationId)
                .Write(response.GroupProtocol)
                .Write(response.LeaderId)
                .Write(response.MemberId)
                .Write(response.Members.Count);

            var encoder = context.GetEncoder();
            foreach (var member in response.Members) {
                writer.Write(member.MemberId)
                       .Write(member.Metadata, encoder);
            }
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, HeartbeatResponse response)
        {
            if (response == null) return false;

            writer.Write(response.ErrorCode);
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, LeaveGroupResponse response)
        {
            if (response == null) return false;

            writer.Write(response.ErrorCode);
            writer.Write(response.ErrorCode);
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, SyncGroupResponse response)
        {
            if (response == null) return false;

            var encoder = context.GetEncoder();
            writer.Write(response.ErrorCode)
                   .Write(response.MemberAssignment, encoder);
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, DescribeGroupsResponse response)
        {
            if (response == null) return false;

            writer.Write(response.Groups.Count);
            foreach (var group in response.Groups) {
                writer.Write(group.ErrorCode)
                    .Write(group.GroupId)
                    .Write(group.State)
                    .Write(group.ProtocolType)
                    .Write(group.Protocol);

                var encoder = context.GetEncoder(group.ProtocolType);
                writer.Write(group.Members.Count);
                foreach (var member in group.Members) {
                    writer.Write(member.MemberId)
                        .Write(member.ClientId)
                        .Write(member.ClientHost)
                        .Write(member.MemberMetadata, encoder)
                        .Write(member.MemberAssignment, encoder);
                }
            }
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, ListGroupsResponse response)
        {
            if (response == null) return false;

            writer.Write(response.ErrorCode)
                .Write(response.Groups.Count);
            foreach (var group in response.Groups) {
                writer.Write(group.GroupId)
                    .Write(group.ProtocolType);
            }
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, SaslHandshakeResponse response)
        {
            if (response == null) return false;

            writer.Write(response.ErrorCode)
                .Write(response.EnabledMechanisms.Count);
            foreach (var mechanism in response.EnabledMechanisms) {
                writer.Write(mechanism);
            }
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, ApiVersionsResponse response)
        {
            if (response == null) return false;

            writer.Write(response.ErrorCode)
                .Write(response.SupportedVersions.Count);
            foreach (var versionSupport in response.SupportedVersions) {
                writer.Write((short)versionSupport.ApiKey)
                    .Write(versionSupport.MinVersion)
                    .Write(versionSupport.MaxVersion);
            }
            return true;
        }

        #endregion

        public static byte[] PrefixWithInt32Length(this byte[] source)
        {
            var destination = new byte[source.Length + 4]; 
            using (var stream = new MemoryStream(destination)) {
                using (var writer = new BigEndianBinaryWriter(stream)) {
                    writer.Write(source.Length);
                }
            }
            Buffer.BlockCopy(source, 0, destination, 4, source.Length);
            return destination;
        }
    }
}