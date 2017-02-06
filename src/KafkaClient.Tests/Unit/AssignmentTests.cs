using System;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Assignment;
using KafkaClient.Common;
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
            router.GetConnectionAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
                  .Returns(_ => Task.FromResult(conn));
            conn.SendAsync(Arg.Any<JoinGroupRequest>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                  .Returns(_ => Task.FromResult(new JoinGroupResponse(ErrorCode.NONE, 1, metadata.AssignmentStrategy, _.Arg<JoinGroupRequest>().member_id, _.Arg<JoinGroupRequest>().member_id, new []{ new JoinGroupResponse.Member(_.Arg<JoinGroupRequest>().member_id, metadata) })));
            conn.SendAsync(Arg.Any<DescribeGroupsRequest>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                  .Returns(_ => Task.FromResult(new DescribeGroupsResponse(null)));

            await Async.Using(new Consumer(router, encoders: ConnectionConfiguration.Defaults.Encoders()),
                async consumer => {
                    try {
                        using (var m = await consumer.JoinConsumerGroupAsync("group", ConsumerEncoder.Protocol, metadata, CancellationToken.None)) {
                            var member = (ConsumerMember) m;
                            await member.SyncGroupAsync(CancellationToken.None);
                        }
                        Assert.Fail("Should have thrown exception");
                    } catch (ArgumentOutOfRangeException ex) when (ex.Message.StartsWith($"Unknown strategy {metadata.AssignmentStrategy} for ProtocolType {ConsumerEncoder.Protocol}")) {
                        // not configured here
                    }
                });
        }

        [Test]
        public async Task AssignmentFoundWhenStrategyExists([Values("type1", "type2")] string strategy)
        {
            var metadata = new ConsumerProtocolMetadata("mine", strategy);

            var router = Substitute.For<IRouter>();
            var conn = Substitute.For<IConnection>();
            router.GetConnectionAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
                  .Returns(_ => Task.FromResult(conn));
            router.SyncGroupAsync(Arg.Any<SyncGroupRequest>(), Arg.Any<IRequestContext>(), Arg.Any<IRetry>(), Arg.Any<CancellationToken>())
                  .Returns(_ => Task.FromResult(new SyncGroupResponse(ErrorCode.NONE, new ConsumerMemberAssignment())));
            conn.SendAsync(Arg.Any<JoinGroupRequest>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                  .Returns(_ => Task.FromResult(new JoinGroupResponse(ErrorCode.NONE, 1, metadata.AssignmentStrategy, _.Arg<JoinGroupRequest>().member_id, _.Arg<JoinGroupRequest>().member_id, new []{ new JoinGroupResponse.Member(_.Arg<JoinGroupRequest>().member_id, metadata) })));
            conn.SendAsync(Arg.Any<DescribeGroupsRequest>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                  .Returns(_ => Task.FromResult(new DescribeGroupsResponse(null)));

            var assignor = Substitute.For<IMembershipAssignor>();
            assignor.AssignmentStrategy.ReturnsForAnyArgs(_ => strategy);
            var encoders = ConnectionConfiguration.Defaults.Encoders(new ConsumerEncoder(new SimpleAssignor(), assignor));
            var consumer = new Consumer(router, encoders: encoders);
            using (consumer) {
                using (var m = await consumer.JoinConsumerGroupAsync("group", ConsumerEncoder.Protocol, metadata, CancellationToken.None)) {
                    var member = (ConsumerMember) m;
                    await member.SyncGroupAsync(CancellationToken.None);
                }
            }
        }

        [Test]
        public async Task AssignorFoundWhenStrategyExists([Values("type1", "type2")] string strategy)
        {
            var metadata = new ConsumerProtocolMetadata("mine", strategy);

            var router = Substitute.For<IRouter>();
            var conn = Substitute.For<IConnection>();
            router.GetConnectionAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
                  .Returns(_ => Task.FromResult(conn));
            router.SyncGroupAsync(Arg.Any<SyncGroupRequest>(), Arg.Any<IRequestContext>(), Arg.Any<IRetry>(), Arg.Any<CancellationToken>())
                  .Returns(_ => Task.FromResult(new SyncGroupResponse(ErrorCode.NONE, new ConsumerMemberAssignment())));
            conn.SendAsync(Arg.Any<JoinGroupRequest>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                  .Returns(_ => Task.FromResult(new JoinGroupResponse(ErrorCode.NONE, 1, metadata.AssignmentStrategy, _.Arg<JoinGroupRequest>().member_id, _.Arg<JoinGroupRequest>().member_id, new []{ new JoinGroupResponse.Member(_.Arg<JoinGroupRequest>().member_id, metadata) })));
            conn.SendAsync(Arg.Any<DescribeGroupsRequest>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                  .Returns(_ => Task.FromResult(new DescribeGroupsResponse(null)));

            var assignor = Substitute.For<IMembershipAssignor>();
            assignor.AssignmentStrategy.ReturnsForAnyArgs(_ => strategy);
            var encoders = ConnectionConfiguration.Defaults.Encoders(new ConsumerEncoder(new SimpleAssignor(), assignor));
            var consumer = new Consumer(router, encoders: encoders);
            using (consumer) {
                using (var m = await consumer.JoinConsumerGroupAsync("group", ConsumerEncoder.Protocol, metadata, CancellationToken.None)) {
                    var member = (ConsumerMember) m;
                    await member.SyncGroupAsync(CancellationToken.None);
                }
            }
        }

        [Test]
        public async Task AssigmentSucceedsWhenStrategyExists()
        {
            var metadata = new ConsumerProtocolMetadata("mine");

            var router = Substitute.For<IRouter>();
            var conn = Substitute.For<IConnection>();
            router.GetConnectionAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
                  .Returns(_ => Task.FromResult(conn));
            router.SyncGroupAsync(Arg.Any<SyncGroupRequest>(), Arg.Any<IRequestContext>(), Arg.Any<IRetry>(), Arg.Any<CancellationToken>())
                  .Returns(_ => Task.FromResult(new SyncGroupResponse(ErrorCode.NONE, new ConsumerMemberAssignment())));
            conn.SendAsync(Arg.Any<JoinGroupRequest>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                  .Returns(_ => Task.FromResult(new JoinGroupResponse(ErrorCode.NONE, 1, metadata.AssignmentStrategy, _.Arg<JoinGroupRequest>().member_id, _.Arg<JoinGroupRequest>().member_id, new []{ new JoinGroupResponse.Member(_.Arg<JoinGroupRequest>().member_id, metadata) })));
            conn.SendAsync(Arg.Any<DescribeGroupsRequest>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                  .Returns(_ => Task.FromResult(new DescribeGroupsResponse(null)));

            var consumer = new Consumer(router, encoders: ConnectionConfiguration.Defaults.Encoders());
            using (consumer) {
                using (var m = await consumer.JoinConsumerGroupAsync("group", ConsumerEncoder.Protocol, metadata, CancellationToken.None)) {
                    var member = (ConsumerMember) m;
                    await member.SyncGroupAsync(CancellationToken.None);
                }
            }
        }

        [Test]
        public void InterfacesAreFormattedWithinProtocol()
        {
            var request = new SyncGroupRequest("group", 5, "member", new[] { new SyncGroupRequest.GroupAssignment("member", new ConsumerMemberAssignment(new[] { new TopicPartition("topic-foo", 0), new TopicPartition("topic", 1) })) });
            var formatted = request.ToString();
            Assert.That(formatted.Contains("topic:topic-foo"));
            Assert.That(formatted.Contains("partition_id:1"));
        }

        // design unit TESTS to write:
        // assignment priority is given to first assignor if multiple available
        // non-leader calls to get assignment data
        // leader does not call to get assignment data
        // sticky assignment ensures:
        // - existing assignments are assigned as before
        // - new assignments are assigned
        // - no member is unassigned (meaning it's possible that existing assignments are moved)
        // reassignment disposes open batches
        // reassignment enables new batches
        // reassignment before batches used does nothing
        // synching without changing assignment does not dispose
        // synching without changing assignment results in no new batches
    }
}