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

        public static T Decode<T>(byte[] payload) where T : class, IRequest
        {
            IRequestContext context;
            using (ReadHeader(payload, out context)) { }

            return Decode<T>(context, payload);
        }

        public static T Decode<T>(IRequestContext context, byte[] payload) where T : class, IRequest
        {
            if (typeof(T) == typeof(FetchRequest)) return (T)FetchRequest(context, payload);
            if (typeof(T) == typeof(MetadataRequest)) return (T)MetadataRequest(context, payload);
            if (typeof(T) == typeof(ProduceRequest)) return (T)ProduceRequest(context, payload);
            if (typeof(T) == typeof(OffsetRequest)) return (T)OffsetRequest(context, payload);
            if (typeof(T) == typeof(OffsetCommitRequest)) return (T)OffsetCommitRequest(context, payload);
            if (typeof(T) == typeof(OffsetFetchRequest)) return (T)OffsetFetchRequest(context, payload);
            if (typeof(T) == typeof(GroupCoordinatorRequest)) return (T)GroupCoordinatorRequest(context, payload);
            if (typeof(T) == typeof(ApiVersionsRequest)) return (T)ApiVersionsRequest(context, payload);
            if (typeof(T) == typeof(StopReplicaRequest)) return (T)StopReplicaRequest(context, payload);
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

                var isEncoded = TryEncodeResponse(writer, context, response as FetchResponse)
                || TryEncodeResponse(writer, context, response as MetadataResponse)
                || TryEncodeResponse(writer, context, response as ProduceResponse)
                || TryEncodeResponse(writer, context, response as OffsetResponse)
                || TryEncodeResponse(writer, context, response as OffsetCommitResponse)
                || TryEncodeResponse(writer, context, response as OffsetFetchResponse)
                || TryEncodeResponse(writer, context, response as GroupCoordinatorResponse)
                || TryEncodeResponse(writer, context, response as ApiVersionsResponse)
                || TryEncodeResponse(writer, context, response as StopReplicaResponse);

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
                    var topicName = stream.ReadInt16String();

                    var partitionCount = stream.ReadInt32();
                    for (var j = 0; j < partitionCount; j++) {
                        var partitionId = stream.ReadInt32();
                        var messages = KafkaEncoder.DecodeMessageSet(stream.ReadIntPrefixedBytes(), partitionId).ToList();

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
                    var topicName = stream.ReadInt16String();

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
                    var topicName = stream.ReadInt16String();

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
                var topicNames = new List<string>();
                var count = stream.ReadInt32();
                for (var t = 0; t < count; t++) {
                    var topicName = stream.ReadInt16String();
                    topicNames.Add(topicName);
                }

                return new MetadataRequest(topicNames);
            }
        }
        
        private static IRequest OffsetCommitRequest(IRequestContext context, byte[] payload)
        {
            using (var stream = ReadHeader(payload)) {
                var consumerGroup = stream.ReadInt16String();
                var generationId = 0;
                string memberId = null; 
                if (context.ApiVersion >= 1) {
                    generationId = stream.ReadInt32();
                    memberId = stream.ReadInt16String();
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
                    var topicName = stream.ReadInt16String();

                    var partitionCount = stream.ReadInt32();
                    for (var p = 0; p < partitionCount; p++) {
                        var partitionId = stream.ReadInt32();
                        var offset = stream.ReadInt64();
                        long? timestamp = null;
                        if (context.ApiVersion == 1) {
                            timestamp = stream.ReadInt64();
                        }
                        var metadata = stream.ReadInt16String();

                        offsetCommits.Add(new OffsetCommit(topicName, partitionId, offset, metadata, timestamp));
                    }
                }

                return new OffsetCommitRequest(consumerGroup, offsetCommits, memberId, generationId, offsetRetention);
            }
        }

        private static IRequest OffsetFetchRequest(IRequestContext context, byte[] payload)
        {
            using (var stream = ReadHeader(payload)) {
                var consumerGroup = stream.ReadInt16String();

                var topics = new List<Topic>();
                var count = stream.ReadInt32();
                for (var t = 0; t < count; t++) {
                    var topicName = stream.ReadInt16String();

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
                var groupId = stream.ReadInt16String();

                return new GroupCoordinatorRequest(groupId);
            }
        }

        private static IRequest ApiVersionsRequest(IRequestContext context, byte[] payload)
        {
            using (var stream = ReadHeader(payload)) {
                return new ApiVersionsRequest();
            }
        }

        private static IRequest StopReplicaRequest(IRequestContext context, byte[] payload)
        {
            using (var stream = ReadHeader(payload)) {
                var controllerId = stream.ReadInt32();
                var controllerEpoch = stream.ReadInt32();
                var shouldDelete = stream.ReadBoolean();

                var count = stream.ReadInt32();
                var topics = new List<Topic>();
                for (var i = 0; i < count; i++) {
                    var topicName = stream.ReadInt16String();
                    var partitionId = stream.ReadInt32();
                    topics.Add(new Topic(topicName, partitionId));
                }

                return new StopReplicaRequest(controllerId, controllerEpoch, topics, shouldDelete);
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
                var clientId = stream.ReadInt16String();

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
                writer.Write(topic.Key, StringPrefixEncoding.Int16);
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
                writer.Write(topic.Key, StringPrefixEncoding.Int16);
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
                writer.Write(topic.Key, StringPrefixEncoding.Int16);
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
                writer.Write(broker.Host, StringPrefixEncoding.Int16);
                writer.Write(broker.Port);
            }

            var groupedTopics = response.Topics.GroupBy(t => new { t.TopicName, t.ErrorCode }).ToList();
            writer.Write(groupedTopics.Count);
            foreach (var topic in groupedTopics) {
                writer.Write((short)topic.Key.ErrorCode);
                writer.Write(topic.Key.TopicName, StringPrefixEncoding.Int16);
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
        
        private static bool TryEncodeResponse(BigEndianBinaryWriter writer, IRequestContext context, OffsetCommitResponse response)
        {
            if (response == null) return false;

            var groupedTopics = response.Topics.GroupBy(t => t.TopicName).ToList();
            writer.Write(groupedTopics.Count);
            foreach (var topic in groupedTopics) {
                writer.Write(topic.Key, StringPrefixEncoding.Int16);
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
                writer.Write(topic.Key, StringPrefixEncoding.Int16);
                var partitions = topic.ToList();
                writer.Write(partitions.Count); // partitionsPerTopic
                foreach (var partition in partitions) {
                    writer.Write(partition.PartitionId);
                    writer.Write(partition.Offset);
                    writer.Write(partition.MetaData, StringPrefixEncoding.Int16);
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
            writer.Write(response.Host, StringPrefixEncoding.Int16);
            writer.Write(response.Port);
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

        private static bool TryEncodeResponse(BigEndianBinaryWriter writer, IRequestContext context, StopReplicaResponse response)
        {
            if (response == null) return false;

            writer.Write((short)response.ErrorCode);
            writer.Write(response.Topics.Count);
            foreach (var topic in response.Topics) {
                writer.Write(topic.TopicName, StringPrefixEncoding.Int16);
                writer.Write(topic.PartitionId);
                writer.Write((short)topic.ErrorCode);
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