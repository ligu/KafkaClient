using System;
using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Assignment;
using KafkaClient.Connections;
using KafkaClient.Protocol;
using NSubstitute;
using NUnit.Framework;

namespace KafkaClient.Tests.Unit
{
    [TestFixture]
    public class AssignmentTests
    {
        [Test]
        public async Task AssignmentThrowsExceptionWhenStrategyNotFound()
        {
            var metadata = new ConsumerProtocolMetadata("mine", "unknown");

            var router = Substitute.For<IRouter>();
            var conn = Substitute.For<IConnection>();
            router.GetGroupBrokerAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                  .Returns(_ => Task.FromResult(new GroupBroker(_.Arg<string>(), 0, conn)));
            conn.SendAsync(Arg.Any<JoinGroupRequest>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                  .Returns(_ => Task.FromResult(new JoinGroupResponse(ErrorResponseCode.None, 1, metadata.AssignmentStrategy, _.Arg<JoinGroupRequest>().MemberId, _.Arg<JoinGroupRequest>().MemberId, new []{ new JoinGroupResponse.Member(_.Arg<JoinGroupRequest>().MemberId, metadata) })));

            var consumer = new Consumer(router, encoders: ConnectionConfiguration.Defaults.Encoders());
            using (consumer) {
                try {
                    using (var member = await consumer.JoinConsumerGroupAsync("group", ConsumerEncoder.Protocol, metadata, CancellationToken.None)) {
                        await consumer.SyncGroupAsync(
                                member.GroupId, member.MemberId, member.GenerationId, member.ProtocolType,
                                ImmutableDictionary<string, IMemberMetadata>.Empty.Add(metadata.AssignmentStrategy, metadata),
                                ImmutableDictionary<string, IMemberAssignment>.Empty,
                                CancellationToken.None)
                            ;
                    }
                    Assert.Fail("Should have thrown exception");
                } catch (ArgumentOutOfRangeException ex) when (ex.Message.StartsWith($"Unknown strategy {metadata.AssignmentStrategy} for ProtocolType {ConsumerEncoder.Protocol}")) {
                    // not configured here
                }
            }
        }

        [Test]
        public async Task AssignmentFoundWhenStrategyExists([Values("type1", "type2")] string strategy)
        {
            var metadata = new ConsumerProtocolMetadata("mine", strategy);

            var router = Substitute.For<IRouter>();
            var conn = Substitute.For<IConnection>();
            router.GetGroupBrokerAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                  .Returns(_ => Task.FromResult(new GroupBroker(_.Arg<string>(), 0, conn)));
            conn.SendAsync(Arg.Any<JoinGroupRequest>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                  .Returns(_ => Task.FromResult(new JoinGroupResponse(ErrorResponseCode.None, 1, metadata.AssignmentStrategy, _.Arg<JoinGroupRequest>().MemberId, _.Arg<JoinGroupRequest>().MemberId, new []{ new JoinGroupResponse.Member(_.Arg<JoinGroupRequest>().MemberId, metadata) })));
            conn.SendAsync(Arg.Any<SyncGroupRequest>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                  .Returns(_ => Task.FromResult(new SyncGroupResponse(ErrorResponseCode.None, new ConsumerMemberAssignment())));
            conn.SendAsync(Arg.Any<DescribeGroupsRequest>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                  .Returns(_ => Task.FromResult(new DescribeGroupsResponse(null)));

            var assignor = Substitute.For<IMembershipAssignor>();
            assignor.AssignmentStrategy.ReturnsForAnyArgs(_ => strategy);
            var encoders = ConnectionConfiguration.Defaults.Encoders(new ConsumerEncoder(new SimpleAssignor(), assignor));
            var consumer = new Consumer(router, encoders: encoders);
            using (consumer) {
                using (var member = await consumer.JoinConsumerGroupAsync("group", ConsumerEncoder.Protocol, metadata, CancellationToken.None)) {
                    await consumer.SyncGroupAsync(
                            member.GroupId, member.MemberId, member.GenerationId, member.ProtocolType,
                            ImmutableDictionary<string, IMemberMetadata>.Empty.Add(metadata.AssignmentStrategy, metadata),
                            ImmutableDictionary<string, IMemberAssignment>.Empty,
                            CancellationToken.None);
                }
            }
        }

        [Test]
        public async Task AssignorFoundWhenStrategyExists([Values("type1", "type2")] string strategy)
        {
            var metadata = new ConsumerProtocolMetadata("mine", strategy);

            var router = Substitute.For<IRouter>();
            var conn = Substitute.For<IConnection>();
            router.GetGroupBrokerAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                  .Returns(_ => Task.FromResult(new GroupBroker(_.Arg<string>(), 0, conn)));
            conn.SendAsync(Arg.Any<JoinGroupRequest>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                  .Returns(_ => Task.FromResult(new JoinGroupResponse(ErrorResponseCode.None, 1, metadata.AssignmentStrategy, _.Arg<JoinGroupRequest>().MemberId, _.Arg<JoinGroupRequest>().MemberId, new []{ new JoinGroupResponse.Member(_.Arg<JoinGroupRequest>().MemberId, metadata) })));
            conn.SendAsync(Arg.Any<SyncGroupRequest>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                  .Returns(_ => Task.FromResult(new SyncGroupResponse(ErrorResponseCode.None, new ConsumerMemberAssignment())));
            conn.SendAsync(Arg.Any<DescribeGroupsRequest>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                  .Returns(_ => Task.FromResult(new DescribeGroupsResponse(null)));

            var assignor = Substitute.For<IMembershipAssignor>();
            assignor.AssignmentStrategy.ReturnsForAnyArgs(_ => strategy);
            var encoders = ConnectionConfiguration.Defaults.Encoders(new ConsumerEncoder(new SimpleAssignor(), assignor));
            var consumer = new Consumer(router, encoders: encoders);
            using (consumer) {
                using (var member = await consumer.JoinConsumerGroupAsync("group", ConsumerEncoder.Protocol, metadata, CancellationToken.None)) {
                    await consumer.SyncGroupAsync(
                            member.GroupId, member.MemberId, member.GenerationId, member.ProtocolType,
                            ImmutableDictionary<string, IMemberMetadata>.Empty.Add(metadata.AssignmentStrategy, metadata),
                            ImmutableDictionary<string, IMemberAssignment>.Empty,
                            CancellationToken.None);
                }
            }
        }

        [Test]
        public async Task AssigmentSucceedsWhenStrategyExists()
        {
            var metadata = new ConsumerProtocolMetadata("mine");

            var router = Substitute.For<IRouter>();
            var conn = Substitute.For<IConnection>();
            router.GetGroupBrokerAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                  .Returns(_ => Task.FromResult(new GroupBroker(_.Arg<string>(), 0, conn)));
            conn.SendAsync(Arg.Any<JoinGroupRequest>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                  .Returns(_ => Task.FromResult(new JoinGroupResponse(ErrorResponseCode.None, 1, metadata.AssignmentStrategy, _.Arg<JoinGroupRequest>().MemberId, _.Arg<JoinGroupRequest>().MemberId, new []{ new JoinGroupResponse.Member(_.Arg<JoinGroupRequest>().MemberId, metadata) })));
            conn.SendAsync(Arg.Any<SyncGroupRequest>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                  .Returns(_ => Task.FromResult(new SyncGroupResponse(ErrorResponseCode.None, new ConsumerMemberAssignment())));
            conn.SendAsync(Arg.Any<DescribeGroupsRequest>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                  .Returns(_ => Task.FromResult(new DescribeGroupsResponse(null)));

            var consumer = new Consumer(router, encoders: ConnectionConfiguration.Defaults.Encoders());
            using (consumer) {
                using (var member = await consumer.JoinConsumerGroupAsync("group", ConsumerEncoder.Protocol, metadata, CancellationToken.None)) {
                    await consumer.SyncGroupAsync(
                            member.GroupId, member.MemberId, member.GenerationId, member.ProtocolType,
                            ImmutableDictionary<string, IMemberMetadata>.Empty.Add(metadata.AssignmentStrategy, metadata),
                            ImmutableDictionary<string, IMemberAssignment>.Empty,
                            CancellationToken.None);
                }
            }
        }

        // design unit TESTS to write:
        // (async) locking is done correctly in the group member
        // linked list crawling for next flows nicely in code
        // dealing correctly with losing ownership
        // can read messages from assigned partition(s)
        // can write a priority based assignor
    }
}