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
        public static void AssertCanEncodeDecodeRequest<T>(this T request, short version, IMembershipEncoder encoder = null, T forComparison = null) where T : class, IRequest
        {
            var encoders = ImmutableDictionary<string, IMembershipEncoder>.Empty;
            if (encoder != null) {
                encoders = encoders.Add(encoder.ProtocolType, encoder);
            }

            var context = new RequestContext(17, version, "Test-Request", encoders, encoder?.ProtocolType);
            var bytes = request.ToBytes(context);
            var decoded = KafkaDecoder.Decode<T>(bytes.Skip(4), context);

            if (forComparison == null) {
                forComparison = request;
            }
            Assert.That(forComparison.GetHashCode(), Is.EqualTo(decoded.GetHashCode()), "HashCode equality");
            Assert.That(forComparison.ShortString(), Is.EqualTo(decoded.ShortString()), "ShortString equality");
            var original = forComparison.ToString();
            var final = decoded.ToString();
            Assert.That(original, Is.EqualTo(final), "ToString equality");
            Assert.That(decoded.Equals(final), Is.False); // general equality test for sanity
            Assert.That(decoded.Equals(decoded), Is.True); // general equality test for sanity
            Assert.That(forComparison.Equals(decoded), $"Original\n{original}\nFinal\n{final}");
        }

        public static void AssertCanEncodeDecodeResponse<T>(this T response, short version, IMembershipEncoder encoder = null, T forComparison = null) where T : class, IResponse
        {
            var encoders = ImmutableDictionary<string, IMembershipEncoder>.Empty;
            if (encoder != null) {
                encoders = encoders.Add(encoder.ProtocolType, encoder);
            }

            var context = new RequestContext(16, version, "Test-Response", encoders, encoder?.ProtocolType);
            var data = KafkaDecoder.EncodeResponseBytes(context, response);
            var decoded = KafkaEncoder.Decode<T>(context, GetType<T>(), data.Skip(KafkaEncoder.ResponseHeaderSize));

            if (forComparison == null) {
                forComparison = response;
            }
            Assert.That(forComparison.GetHashCode(), Is.EqualTo(decoded.GetHashCode()), "HashCode equality");
            var original = forComparison.ToString();
            var final = decoded.ToString();
            Assert.That(original, Is.EqualTo(final), "ToString equality");
            Assert.That(decoded.Equals(final), Is.False); // general test for equality
            Assert.That(decoded.Equals(decoded), Is.True); // general equality test for sanity
            Assert.That(forComparison.Equals(decoded), $"Original\n{original}\nFinal\n{final}");
            Assert.That(forComparison.Errors.HasEqualElementsInOrder(decoded.Errors), "Errors");
        }

        public static ApiKey GetType<T>() where T : class, IResponse
        {
            if (typeof(T) == typeof(ProduceResponse)) return ApiKey.Produce;
            if (typeof(T) == typeof(FetchResponse)) return ApiKey.Fetch;
            if (typeof(T) == typeof(OffsetsResponse)) return ApiKey.Offsets;
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