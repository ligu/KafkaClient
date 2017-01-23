using System;
using System.Collections.Immutable;
using KafkaClient.Assignment;
using KafkaClient.Common;
using KafkaClient.Protocol;
using NUnit.Framework;

namespace KafkaClient.Tests
{
    public static class ProtocolAssertionExtensions
    {
        public static void AssertCanEncodeDecodeRequest<T>(this T request, short version, IMembershipEncoder encoder = null) where T : class, IRequest
        {
            var encoders = ImmutableDictionary<string, IMembershipEncoder>.Empty;
            if (encoder != null) {
                encoders = encoders.Add(encoder.ProtocolType, encoder);
            }

            var context = new RequestContext(17, version, "Test-Request", encoders, encoder?.ProtocolType);
            var data = KafkaEncoder.Encode(context, request);
            var decoded = KafkaDecoder.Decode<T>(data.Skip(4), context);

            if (!request.Equals(decoded)) {
                var original = request.ToFormattedString();
                var final = decoded.ToFormattedString();
                Console.WriteLine($"Original\n{original}\nFinal\n{final}");
                Assert.That(final, Is.EqualTo(original));
                Assert.Fail("Not equal, although strings suggest they are?");
            }
        }

        public static void AssertCanEncodeDecodeResponse<T>(this T response, short version, IMembershipEncoder encoder = null) where T : class, IResponse
        {
            var encoders = ImmutableDictionary<string, IMembershipEncoder>.Empty;
            if (encoder != null) {
                encoders = encoders.Add(encoder.ProtocolType, encoder);
            }

            var context = new RequestContext(16, version, "Test-Response", encoders, encoder?.ProtocolType);
            var data = KafkaDecoder.EncodeResponseBytes(context, response);
            var decoded = KafkaEncoder.Decode<T>(context, GetType<T>(), data.Skip(KafkaEncoder.ResponseHeaderSize));

            if (!response.Equals(decoded)) {
                var original = response.ToFormattedString();
                var final = decoded.ToFormattedString();
                Console.WriteLine($"Original\n{original}\nFinal\n{final}");
                Assert.That(final, Is.EqualTo(original));
                Assert.Fail("Not equal, although strings suggest they are?");
            }
        }

        public static ApiKeyRequestType GetType<T>() where T : class, IResponse
        {
            if (typeof(T) == typeof(ProduceResponse)) return ApiKeyRequestType.Produce;
            if (typeof(T) == typeof(FetchResponse)) return ApiKeyRequestType.Fetch;
            if (typeof(T) == typeof(OffsetResponse)) return ApiKeyRequestType.Offset;
            if (typeof(T) == typeof(MetadataResponse)) return ApiKeyRequestType.Metadata;
            if (typeof(T) == typeof(OffsetCommitResponse)) return ApiKeyRequestType.OffsetCommit;
            if (typeof(T) == typeof(OffsetFetchResponse)) return ApiKeyRequestType.OffsetFetch;
            if (typeof(T) == typeof(GroupCoordinatorResponse)) return ApiKeyRequestType.GroupCoordinator;
            if (typeof(T) == typeof(JoinGroupResponse)) return ApiKeyRequestType.JoinGroup;
            if (typeof(T) == typeof(HeartbeatResponse)) return ApiKeyRequestType.Heartbeat;
            if (typeof(T) == typeof(LeaveGroupResponse)) return ApiKeyRequestType.LeaveGroup;
            if (typeof(T) == typeof(SyncGroupResponse)) return ApiKeyRequestType.SyncGroup;
            if (typeof(T) == typeof(DescribeGroupsResponse)) return ApiKeyRequestType.DescribeGroups;
            if (typeof(T) == typeof(ListGroupsResponse)) return ApiKeyRequestType.ListGroups;
            if (typeof(T) == typeof(SaslHandshakeResponse)) return ApiKeyRequestType.SaslHandshake;
            if (typeof(T) == typeof(ApiVersionsResponse)) return ApiKeyRequestType.ApiVersions;
            if (typeof(T) == typeof(CreateTopicsResponse)) return ApiKeyRequestType.CreateTopics;
            if (typeof(T) == typeof(DeleteTopicsResponse)) return ApiKeyRequestType.DeleteTopics;
            throw new InvalidOperationException();
        }

    }
}