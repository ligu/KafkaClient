using System;
using System.Collections.Immutable;
using KafkaClient.Assignment;
using KafkaClient.Common;
using KafkaClient.Protocol;
using KafkaClient.Testing;
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

            Assert.That(request.GetHashCode(), Is.EqualTo(decoded.GetHashCode()), "HashCode equality");
            Assert.That(request.ShortString(), Is.EqualTo(decoded.ShortString()), "ShortString equality");
            var original = request.ToString();
            var final = decoded.ToString();
            Assert.That(original, Is.EqualTo(final), "ToString equality");
            Assert.That(request, Is.EqualTo(decoded), $"Original\n{original}\nFinal\n{final}");
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

            Assert.That(response.GetHashCode(), Is.EqualTo(decoded.GetHashCode()), "HashCode equality");
            var original = response.ToString();
            var final = decoded.ToString();
            Assert.That(original, Is.EqualTo(final), "ToString equality");
            Assert.That(response, Is.EqualTo(decoded), $"Original\n{original}\nFinal\n{final}");
        }

        public static ApiKey GetType<T>() where T : class, IResponse
        {
            if (typeof(T) == typeof(ProduceResponse)) return ApiKey.Produce;
            if (typeof(T) == typeof(FetchResponse)) return ApiKey.Fetch;
            if (typeof(T) == typeof(OffsetResponse)) return ApiKey.Offset;
            if (typeof(T) == typeof(MetadataResponse)) return ApiKey.Metadata;
            if (typeof(T) == typeof(OffsetCommitResponse)) return ApiKey.OffsetCommit;
            if (typeof(T) == typeof(OffsetFetchResponse)) return ApiKey.OffsetFetch;
            if (typeof(T) == typeof(GroupCoordinatorResponse)) return ApiKey.GroupCoordinator;
            if (typeof(T) == typeof(JoinGroupResponse)) return ApiKey.JoinGroup;
            if (typeof(T) == typeof(HeartbeatResponse)) return ApiKey.Heartbeat;
            if (typeof(T) == typeof(LeaveGroupResponse)) return ApiKey.LeaveGroup;
            if (typeof(T) == typeof(SyncGroupResponse)) return ApiKey.SyncGroup;
            if (typeof(T) == typeof(DescribeGroupsResponse)) return ApiKey.DescribeGroups;
            if (typeof(T) == typeof(ListGroupsResponse)) return ApiKey.ListGroups;
            if (typeof(T) == typeof(SaslHandshakeResponse)) return ApiKey.SaslHandshake;
            if (typeof(T) == typeof(ApiVersionsResponse)) return ApiKey.ApiVersions;
            if (typeof(T) == typeof(CreateTopicsResponse)) return ApiKey.CreateTopics;
            if (typeof(T) == typeof(DeleteTopicsResponse)) return ApiKey.DeleteTopics;
            throw new InvalidOperationException();
        }

    }
}