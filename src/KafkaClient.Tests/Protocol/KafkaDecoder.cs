using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using KafkaClient.Common;
using KafkaClient.Protocol;

namespace KafkaClient.Tests.Protocol
{
    public static class KafkaDecoder
    {
        public static IRequestContext DecodeHeader(byte[] data)
        {
            IRequestContext context;
            using (ReadHeader(data, out context)) {
                return context;
            }            
        }

        public static T Decode<T>(byte[] payload, IRequestContext context = null) where T : class, IRequest
        {
            var protocolType = context?.ProtocolType;
            var encoders = context?.Encoders;
            using (ReadHeader(payload, out context)) { }

            return Decode<T>(new RequestContext(context.CorrelationId, context.ApiVersion, context.ClientId, encoders, protocolType), payload);
        }

        public static T Decode<T>(IRequestContext context, byte[] payload) where T : class, IRequest
        {
            if (typeof(T) == typeof(ProduceRequest)) return (T)ProduceRequest(context, payload);
            if (typeof(T) == typeof(FetchRequest)) return (T)FetchRequest(context, payload);
            if (typeof(T) == typeof(OffsetRequest)) return (T)OffsetRequest(context, payload);
            if (typeof(T) == typeof(MetadataRequest)) return (T)MetadataRequest(context, payload);
            if (typeof(T) == typeof(StopReplicaRequest)) return (T)StopReplicaRequest(context, payload);
            if (typeof(T) == typeof(OffsetCommitRequest)) return (T)OffsetCommitRequest(context, payload);
            if (typeof(T) == typeof(OffsetFetchRequest)) return (T)OffsetFetchRequest(context, payload);
            if (typeof(T) == typeof(GroupCoordinatorRequest)) return (T)GroupCoordinatorRequest(context, payload);
            if (typeof(T) == typeof(JoinGroupRequest)) return (T)JoinGroupRequest(context, payload);
            if (typeof(T) == typeof(HeartbeatRequest)) return (T)HeartbeatRequest(context, payload);
            if (typeof(T) == typeof(LeaveGroupRequest)) return (T)LeaveGroupRequest(context, payload);
            if (typeof(T) == typeof(SyncGroupRequest)) return (T)SyncGroupRequest(context, payload);
            if (typeof(T) == typeof(DescribeGroupsRequest)) return (T)DescribeGroupsRequest(context, payload);
            if (typeof(T) == typeof(ListGroupsRequest)) return (T)ListGroupsRequest(context, payload);
            if (typeof(T) == typeof(SaslHandshakeRequest)) return (T)SaslHandshakeRequest(context, payload);
            if (typeof(T) == typeof(ApiVersionsRequest)) return (T)ApiVersionsRequest(context, payload);
            return default(T);
        }

        public static byte[] EncodeResponseBytes<T>(IRequestContext context, T response) where T : IResponse
        {
            byte[] data = null;
            using (var stream = new MemoryStream()) {
                var writer = new BigEndianBinaryWriter(stream);
                writer.Write(0); // size placeholder
                // From http://kafka.apache.org/protocol.html#protocol_messages
                // 
                // Response Header => correlation_id 
                //  correlation_id => INT32  -- The user-supplied value passed in with the request
                writer.Write(context.CorrelationId);

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

                data = new byte[stream.Position];
                stream.Position = 0;
                writer.Write(data.Length - 4);
                Buffer.BlockCopy(stream.GetBuffer(), 0, data, 0, data.Length);
            }
            return data;
        }

        #region Decode

        private static IRequest ProduceRequest(IRequestContext context, byte[] data)
        {
            using (var stream = ReadHeader(data)) {
                var acks = stream.ReadInt16();
                var timeout = stream.ReadInt32();

                var payloads = new List<Payload>();
                var payloadCount = stream.ReadInt32();
                for (var i = 0; i < payloadCount; i++) {
                    var topicName = stream.ReadString();

                    var partitionCount = stream.ReadInt32();
                    for (var j = 0; j < partitionCount; j++) {
                        var partitionId = stream.ReadInt32();
                        var messages = KafkaEncoder.DecodeMessageSet(stream.ReadBytes(), partitionId).ToList();

                        payloads.Add(new Payload(topicName, partitionId, messages));
                    }
                }
                return new ProduceRequest(payloads, TimeSpan.FromMilliseconds(timeout), acks);
            }
        }

        private static IRequest FetchRequest(IRequestContext context, byte[] data)
        {
            using (var stream = ReadHeader(data)) {
                var replicaId = stream.ReadInt32(); // expect -1
                var maxWaitTime = stream.ReadInt32();
                var minBytes = stream.ReadInt32();

                var fetches = new List<Fetch>();
                var payloadCount = stream.ReadInt32();
                for (var i = 0; i < payloadCount; i++) {
                    var topicName = stream.ReadString();

                    var partitionCount = stream.ReadInt32();
                    for (var j = 0; j < partitionCount; j++) {
                        var partitionId = stream.ReadInt32();
                        var offset = stream.ReadInt64();
                        var maxBytes = stream.ReadInt32();

                        fetches.Add(new Fetch(topicName, partitionId, offset, maxBytes));
                    }
                }
                return new FetchRequest(fetches, TimeSpan.FromMilliseconds(maxWaitTime), minBytes);
            }
        }

        private static IRequest OffsetRequest(IRequestContext context, byte[] data)
        {
            using (var stream = ReadHeader(data)) {
                var replicaId = stream.ReadInt32(); // expect -1

                var offsets = new List<Offset>();
                var offsetCount = stream.ReadInt32();
                for (var i = 0; i < offsetCount; i++) {
                    var topicName = stream.ReadString();

                    var partitionCount = stream.ReadInt32();
                    for (var j = 0; j < partitionCount; j++) {
                        var partitionId = stream.ReadInt32();
                        var time = stream.ReadInt64();
                        var maxOffsets = stream.ReadInt32();

                        offsets.Add(new Offset(topicName, partitionId, time, maxOffsets));
                    }
                }
                return new OffsetRequest(offsets);
            }
        }

        private static IRequest MetadataRequest(IRequestContext context, byte[] data)
        {
            using (var stream = ReadHeader(data)) {
                var topicNames = new string[stream.ReadInt32()];
                for (var t = 0; t < topicNames.Length; t++) {
                    topicNames[t] = stream.ReadString();
                }

                return new MetadataRequest(topicNames);
            }
        }

        private static IRequest StopReplicaRequest(IRequestContext context, byte[] payload)
        {
            using (var stream = ReadHeader(payload)) {
                var controllerId = stream.ReadInt32();
                var controllerEpoch = stream.ReadInt32();
                var shouldDelete = stream.ReadBoolean();

                var topics = new Topic[stream.ReadInt32()];
                for (var t = 0; t < topics.Length; t++) {
                    var topicName = stream.ReadString();
                    var partitionId = stream.ReadInt32();
                    topics[t] = new Topic(topicName, partitionId);
                }

                return new StopReplicaRequest(controllerId, controllerEpoch, topics, shouldDelete);
            }
        }
        
        private static IRequest OffsetCommitRequest(IRequestContext context, byte[] payload)
        {
            using (var stream = ReadHeader(payload)) {
                var consumerGroup = stream.ReadString();
                var generationId = 0;
                string memberId = null; 
                if (context.ApiVersion >= 1) {
                    generationId = stream.ReadInt32();
                    memberId = stream.ReadString();
                }
                TimeSpan? offsetRetention = null;
                if (context.ApiVersion >= 2) {
                    var retentionTime = stream.ReadInt64();
                    if (retentionTime >= 0) {
                        offsetRetention = TimeSpan.FromMilliseconds(retentionTime);
                    }
                }

                var offsetCommits = new List<OffsetCommit>();
                var count = stream.ReadInt32();
                for (var o = 0; o < count; o++) {
                    var topicName = stream.ReadString();

                    var partitionCount = stream.ReadInt32();
                    for (var p = 0; p < partitionCount; p++) {
                        var partitionId = stream.ReadInt32();
                        var offset = stream.ReadInt64();
                        long? timestamp = null;
                        if (context.ApiVersion == 1) {
                            timestamp = stream.ReadInt64();
                        }
                        var metadata = stream.ReadString();

                        offsetCommits.Add(new OffsetCommit(topicName, partitionId, offset, metadata, timestamp));
                    }
                }

                return new OffsetCommitRequest(consumerGroup, offsetCommits, memberId, generationId, offsetRetention);
            }
        }

        private static IRequest OffsetFetchRequest(IRequestContext context, byte[] payload)
        {
            using (var stream = ReadHeader(payload)) {
                var consumerGroup = stream.ReadString();

                var topics = new List<Topic>();
                var count = stream.ReadInt32();
                for (var t = 0; t < count; t++) {
                    var topicName = stream.ReadString();

                    var partitionCount = stream.ReadInt32();
                    for (var p = 0; p < partitionCount; p++) {
                        var partitionId = stream.ReadInt32();

                        topics.Add(new Topic(topicName, partitionId));
                    }
                }

                return new OffsetFetchRequest(consumerGroup, topics);
            }
        }
        
        private static IRequest GroupCoordinatorRequest(IRequestContext context, byte[] payload)
        {
            using (var stream = ReadHeader(payload)) {
                var groupId = stream.ReadString();

                return new GroupCoordinatorRequest(groupId);
            }
        }

        private static IRequest JoinGroupRequest(IRequestContext context, byte[] payload)
        {
            using (var stream = ReadHeader(payload)) {
                var groupId = stream.ReadString();
                var sessionTimeout = TimeSpan.FromMilliseconds(stream.ReadInt32());
                var memberId = stream.ReadString();
                var protocolType = stream.ReadString();
                var groupProtocols = new GroupProtocol[stream.ReadInt32()];

                var encoder = context.GetEncoder(protocolType);
                for (var g = 0; g < groupProtocols.Length; g++) {
                    var protocolName = stream.ReadString();
                    var metadata = encoder.DecodeMetadata(stream.ReadBytes());
                    groupProtocols[g] = new GroupProtocol(protocolName, metadata);
                }

                return new JoinGroupRequest(groupId, sessionTimeout, memberId, protocolType, groupProtocols);
            }
        }

        private static IRequest HeartbeatRequest(IRequestContext context, byte[] payload)
        {
            using (var stream = ReadHeader(payload)) {
                var groupId = stream.ReadString();
                var generationId = stream.ReadInt32();
                var memberId = stream.ReadString();

                return new HeartbeatRequest(groupId, generationId, memberId);
            }
        }

        private static IRequest LeaveGroupRequest(IRequestContext context, byte[] payload)
        {
            using (var stream = ReadHeader(payload)) {
                var groupId = stream.ReadString();
                var memberId = stream.ReadString();

                return new LeaveGroupRequest(groupId, memberId);
            }
        }

        private static IRequest SyncGroupRequest(IRequestContext context, byte[] payload)
        {
            using (var stream = ReadHeader(payload)) {
                var groupId = stream.ReadString();
                var generationId = stream.ReadInt32();
                var memberId = stream.ReadString();

                var encoder = context.GetEncoder();
                var groupAssignments = new SyncGroupAssignment[stream.ReadInt32()];
                for (var a = 0; a < groupAssignments.Length; a++) {
                    var groupMemberId = stream.ReadString();
                    var assignment = encoder.DecodeAssignment(stream.ReadBytes());

                    groupAssignments[a] = new SyncGroupAssignment(groupMemberId, assignment);
                }

                return new SyncGroupRequest(groupId, generationId, memberId, groupAssignments);
            }
        }

        private static IRequest DescribeGroupsRequest(IRequestContext context, byte[] payload)
        {
            using (var stream = ReadHeader(payload)) {
                var groupIds = new string[stream.ReadInt32()];
                for (var i = 0; i < groupIds.Length; i++) {
                    groupIds[i] = stream.ReadString();
                }

                return new DescribeGroupsRequest(groupIds);
            }
        }

        private static IRequest ListGroupsRequest(IRequestContext context, byte[] payload)
        {
            using (var stream = ReadHeader(payload)) {
                return new ListGroupsRequest();
            }
        }

        private static IRequest SaslHandshakeRequest(IRequestContext context, byte[] payload)
        {
            using (var stream = ReadHeader(payload)) {
                var mechanism = stream.ReadString();
                return new SaslHandshakeRequest(mechanism);
            }
        }

        private static IRequest ApiVersionsRequest(IRequestContext context, byte[] payload)
        {
            using (var stream = ReadHeader(payload)) {
                return new ApiVersionsRequest();
            }
        }
        
        private static BigEndianBinaryReader ReadHeader(byte[] data)
        {
            IRequestContext context;
            return ReadHeader(data, out context);
        }

        private static BigEndianBinaryReader ReadHeader(byte[] data, out IRequestContext context)
        {
            var stream = new BigEndianBinaryReader(data, 4);
            try {
                var apiKey = (ApiKeyRequestType) stream.ReadInt16();
                var version = stream.ReadInt16();
                var correlationId = stream.ReadInt32();
                var clientId = stream.ReadString();

                context = new RequestContext(correlationId, version, clientId);
            } catch {
                context = null;
                stream?.Dispose();
                stream = null;
            }
            return stream;
        } 

        #endregion

        #region Encode

        private static bool TryEncodeResponse(BigEndianBinaryWriter writer, IRequestContext context, ProduceResponse response)
        {
            if (response == null) return false;

            var groupedTopics = response.Topics.GroupBy(t => t.TopicName).ToList();
            writer.Write(groupedTopics.Count);
            foreach (var topic in groupedTopics) {
                writer.Write(topic.Key);
                var partitions = topic.ToList();

                writer.Write(partitions.Count); // partitionsPerTopic
                foreach (var partition in partitions) {
                    writer.Write(partition.PartitionId);
                    writer.Write((short)partition.ErrorCode);
                    writer.Write(partition.Offset);
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

        private static bool TryEncodeResponse(BigEndianBinaryWriter writer, IRequestContext context, FetchResponse response)
        {
            if (response == null) return false;

            if (context.ApiVersion >= 1) {
                writer.Write((int?)response.ThrottleTime?.TotalMilliseconds ?? 0);
            }
            var groupedTopics = response.Topics.GroupBy(t => t.TopicName).ToList();
            writer.Write(groupedTopics.Count);
            foreach (var topic in groupedTopics) {
                writer.Write(topic.Key);
                var partitions = topic.ToList();

                writer.Write(partitions.Count); // partitionsPerTopic
                foreach (var partition in partitions) {
                    writer.Write(partition.PartitionId);
                    writer.Write((short)partition.ErrorCode);
                    writer.Write(partition.HighWaterMark);

                    var messageSet = KafkaEncoder.EncodeMessageSet(partition.Messages);
                    writer.Write(messageSet.Length);
                    writer.Write(messageSet);
                }
            }
            return true;
        }

        private static bool TryEncodeResponse(BigEndianBinaryWriter writer, IRequestContext context, OffsetResponse response)
        {
            if (response == null) return false;

            var groupedTopics = response.Topics.GroupBy(t => t.TopicName).ToList();
            writer.Write(groupedTopics.Count);
            foreach (var topic in groupedTopics) {
                writer.Write(topic.Key);
                var partitions = topic.ToList();

                writer.Write(partitions.Count); // partitionsPerTopic
                foreach (var partition in partitions) {
                    writer.Write(partition.PartitionId);
                    writer.Write((short)partition.ErrorCode);
                    writer.Write(partition.Offsets.Count);
                    foreach (var offset in partition.Offsets) {
                        writer.Write(offset);
                    }
                }
            }
            return true;
        }

        private static bool TryEncodeResponse(BigEndianBinaryWriter writer, IRequestContext context, MetadataResponse response)
        {
            if (response == null) return false;

            writer.Write(response.Brokers.Count);
            foreach (var broker in response.Brokers) {
                writer.Write(broker.BrokerId);
                writer.Write(broker.Host);
                writer.Write(broker.Port);
            }

            var groupedTopics = response.Topics.GroupBy(t => new { t.TopicName, t.ErrorCode }).ToList();
            writer.Write(groupedTopics.Count);
            foreach (var topic in groupedTopics) {
                writer.Write((short)topic.Key.ErrorCode);
                writer.Write(topic.Key.TopicName);
                var partitions = topic.SelectMany(_ => _.Partitions).ToList();
                writer.Write(partitions.Count); // partitionsPerTopic
                foreach (var partition in partitions) {
                    writer.Write((short)partition.ErrorCode);
                    writer.Write(partition.PartitionId);
                    writer.Write(partition.LeaderId);
                    writer.Write(partition.Replicas.Count);
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

        private static bool TryEncodeResponse(BigEndianBinaryWriter writer, IRequestContext context, StopReplicaResponse response)
        {
            if (response == null) return false;

            writer.Write((short)response.ErrorCode);
            writer.Write(response.Topics.Count);
            foreach (var topic in response.Topics) {
                writer.Write(topic.TopicName);
                writer.Write(topic.PartitionId);
                writer.Write((short)topic.ErrorCode);
            }
            return true;
        }
        
        private static bool TryEncodeResponse(BigEndianBinaryWriter writer, IRequestContext context, OffsetCommitResponse response)
        {
            if (response == null) return false;

            var groupedTopics = response.Topics.GroupBy(t => t.TopicName).ToList();
            writer.Write(groupedTopics.Count);
            foreach (var topic in groupedTopics) {
                writer.Write(topic.Key);
                var partitions = topic.ToList();
                writer.Write(partitions.Count); // partitionsPerTopic
                foreach (var partition in partitions) {
                    writer.Write(partition.PartitionId);
                    writer.Write((short)partition.ErrorCode);
                }
            }
            return true;
        }

        private static bool TryEncodeResponse(BigEndianBinaryWriter writer, IRequestContext context, OffsetFetchResponse response)
        {
            if (response == null) return false;

            var groupedTopics = response.Topics.GroupBy(t => t.TopicName).ToList();
            writer.Write(groupedTopics.Count);
            foreach (var topic in groupedTopics) {
                writer.Write(topic.Key);
                var partitions = topic.ToList();
                writer.Write(partitions.Count); // partitionsPerTopic
                foreach (var partition in partitions) {
                    writer.Write(partition.PartitionId);
                    writer.Write(partition.Offset);
                    writer.Write(partition.MetaData);
                    writer.Write((short)partition.ErrorCode);
                }
            }
            return true;
        }
        
        private static bool TryEncodeResponse(BigEndianBinaryWriter writer, IRequestContext context, GroupCoordinatorResponse response)
        {
            if (response == null) return false;

            writer.Write((short)response.ErrorCode);
            writer.Write(response.BrokerId);
            writer.Write(response.Host);
            writer.Write(response.Port);
            return true;
        }

        private static bool TryEncodeResponse(BigEndianBinaryWriter writer, IRequestContext context, JoinGroupResponse response)
        {
            if (response == null) return false;

            writer.Write((short)response.ErrorCode);
            writer.Write(response.GenerationId);
            writer.Write(response.GroupProtocol);
            writer.Write(response.LeaderId);
            writer.Write(response.MemberId);
            writer.Write(response.Members.Count);

            var encoder = context.GetEncoder(response.GroupProtocol);
            foreach (var member in response.Members) {
                writer.Write(member.MemberId);
                writer.Write(encoder.EncodeMetadata(member.Metadata), false);
            }
            return true;
        }

        private static bool TryEncodeResponse(BigEndianBinaryWriter writer, IRequestContext context, HeartbeatResponse response)
        {
            if (response == null) return false;

            writer.Write((short)response.ErrorCode);
            return true;
        }

        private static bool TryEncodeResponse(BigEndianBinaryWriter writer, IRequestContext context, LeaveGroupResponse response)
        {
            if (response == null) return false;

            writer.Write((short)response.ErrorCode);
            return true;
        }

        private static bool TryEncodeResponse(BigEndianBinaryWriter writer, IRequestContext context, SyncGroupResponse response)
        {
            if (response == null) return false;

            writer.Write((short)response.ErrorCode);
            var encoder = context.GetEncoder();
            writer.Write(encoder.EncodeAssignment(response.MemberAssignment));
            return true;
        }

        private static bool TryEncodeResponse(BigEndianBinaryWriter writer, IRequestContext context, DescribeGroupsResponse response)
        {
            if (response == null) return false;

            writer.Write(response.Groups.Count);
            foreach (var group in response.Groups) {
                writer.Write((short)group.ErrorCode);
                writer.Write(group.GroupId);
                writer.Write(group.State);
                writer.Write(group.ProtocolType);
                writer.Write(group.Protocol);

                var encoder = context.GetEncoder(group.ProtocolType);
                writer.Write(group.Members.Count);
                foreach (var member in group.Members) {
                    writer.Write(member.MemberId);
                    writer.Write(member.ClientId);
                    writer.Write(member.ClientHost);
                    writer.Write(encoder.EncodeMetadata(member.MemberMetadata));
                    writer.Write(encoder.EncodeAssignment(member.MemberAssignment));
                }
            }
            return true;
        }

        private static bool TryEncodeResponse(BigEndianBinaryWriter writer, IRequestContext context, ListGroupsResponse response)
        {
            if (response == null) return false;

            writer.Write((short)response.ErrorCode);
            writer.Write(response.Groups.Count);
            foreach (var group in response.Groups) {
                writer.Write(group.GroupId);
                writer.Write(group.ProtocolType);
            }
            return true;
        }

        private static bool TryEncodeResponse(BigEndianBinaryWriter writer, IRequestContext context, SaslHandshakeResponse response)
        {
            if (response == null) return false;

            writer.Write((short)response.ErrorCode);
            writer.Write(response.EnabledMechanisms.Count);
            foreach (var mechanism in response.EnabledMechanisms) {
                writer.Write(mechanism);
            }
            return true;
        }

        private static bool TryEncodeResponse(BigEndianBinaryWriter writer, IRequestContext context, ApiVersionsResponse response)
        {
            if (response == null) return false;

            writer.Write((short)response.ErrorCode);
            writer.Write(response.SupportedVersions.Count);
            foreach (var versionSupport in response.SupportedVersions) {
                writer.Write((short)versionSupport.ApiKey);
                writer.Write(versionSupport.MinVersion);
                writer.Write(versionSupport.MaxVersion);
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