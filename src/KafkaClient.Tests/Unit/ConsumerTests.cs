using System;
using System.Collections.Immutable;
using System.Linq;
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
    public class ConsumerTests
    {
//        [Test]
//        public async Task CancellationShouldInterruptConsumption()
//        {
//            var scenario = new RoutingScenario();
//#pragma warning disable 1998
//            scenario.Connection1.Add(ApiKey.Fetch, async context => new FetchResponse(new FetchResponse.Topic[] { }));
//#pragma warning restore 1998

//            var router = scenario.CreateRouter();
//            var consumer = new Consumer(router);
//            var tokenSrc = new CancellationTokenSource();

//            var consumeTask = consumer.FetchBatchAsync("TestTopic", 0, 0, tokenSrc.Token, 2048);

//            //wait until the fake broker is running and requesting fetches
//            var wait = await TaskTest.WaitFor(() => scenario.Connection1[ApiKey.Fetch] > 10);

//            tokenSrc.Cancel();

//            try
//            {
//                await consumeTask;
//                Assert.Fail("Should throw OperationFailedException");
//            }
//            catch (AggregateException ex) when (ex.InnerException is OperationCanceledException)
//            {
//            }
//        }

        [Test]
        public async Task EnsureConsumerDisposesRouter()
        {
            var router = Substitute.For<IRouter>();

            var consumer = new Consumer(router, leaveRouterOpen: false);
            await consumer.DisposeAsync();
#pragma warning disable 4014
            router.Received(1).DisposeAsync();
#pragma warning restore 4014
        }

        [Test]
        public async Task EnsureConsumerDoesNotDisposeRouter()
        {
            var router = Substitute.For<IRouter>();
            var consumer = new Consumer(router);
            await consumer.DisposeAsync();
#pragma warning disable 4014
            router.DidNotReceive().DisposeAsync();
#pragma warning restore 4014
            router.DidNotReceive().Dispose();
        }

        [Test]
        public async Task ConsumerThowsArgumentExceptionWhenMemberMetadataIsNotKnownByConsumer([Values(null, "", "unknown")] string protocolType)
        {
            var router = Substitute.For<IRouter>();

            var consumer = new Consumer(router);
            using (consumer) {
                try {
                    await consumer.JoinConsumerGroupAsync("group", protocolType, new ByteTypeMetadata("mine", new ArraySegment<byte>()), CancellationToken.None);
                    Assert.Fail("Should have thrown exception");
                } catch (ArgumentOutOfRangeException ex) {
                    Assert.That(ex.Message.StartsWith($"ProtocolType {protocolType} is unknown"));
                }
            }
        }

        [Test]
        public async Task ConsumerDoesNotThowArgumentExceptionWhenMemberMetadataIsKnownByConsumer()
        {
            var router = Substitute.For<IRouter>();
            var conn = Substitute.For<IConnection>();
            router.GetGroupBrokerAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                  .Returns(_ => Task.FromResult(new GroupBroker(_.Arg<string>(), 0, conn)));
            router.Configuration.Returns(new RouterConfiguration(refreshRetry: new Retry(1, TimeSpan.FromSeconds(2))));

            var consumer = new Consumer(router, new ConsumerConfiguration(coordinationRetry: Retry.AtMost(2)), encoders: ConnectionConfiguration.Defaults.Encoders());
            using (consumer) {
                try {
                    await consumer.JoinConsumerGroupAsync("group", ConsumerEncoder.Protocol, new ByteTypeMetadata("mine", new ArraySegment<byte>()), CancellationToken.None);
                } catch (RequestException) {
                    // since the servers aren't available
                }
            }
        }

        [Test]
        public async Task ConsumerSyncsGroupAfterJoining()
        {
            var protocol = new JoinGroupRequest.GroupProtocol(new ConsumerProtocolMetadata("mine"));
            var router = Substitute.For<IRouter>();
            var conn = Substitute.For<IConnection>();
            router.GetGroupBrokerAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                  .Returns(_ => Task.FromResult(new GroupBroker(_.Arg<string>(), 0, conn)));            
            router.SyncGroupAsync(Arg.Any<SyncGroupRequest>(), Arg.Any<IRequestContext>(), Arg.Any<IRetry>(), Arg.Any<CancellationToken>())
                .Returns(_ => Task.FromResult(new SyncGroupResponse(ErrorCode.None, new ConsumerMemberAssignment(new [] { new TopicPartition("name", 0) }))));

            var consumer = new Consumer(router);
            var request = new JoinGroupRequest(TestConfig.GroupId(), TimeSpan.FromSeconds(30), "", ConsumerEncoder.Protocol, new [] { protocol });
            var memberId = Guid.NewGuid().ToString("N");
            var response = new JoinGroupResponse(ErrorCode.None, 1, protocol.Name, memberId, memberId, new []{ new JoinGroupResponse.Member(memberId, new ConsumerProtocolMetadata("mine")) });

            using (new ConsumerMember(consumer, request, response, log: TestConfig.Log)) {
                await Task.Delay(300);
            }

#pragma warning disable 4014
            router.Received().SyncGroupAsync(
                Arg.Is((SyncGroupRequest s) => s.GroupId == request.GroupId && s.MemberId == memberId),
                Arg.Any<IRequestContext>(),
                Arg.Any<IRetry>(), 
                Arg.Any<CancellationToken>());
            conn.DidNotReceive().SendAsync(
                Arg.Is((HeartbeatRequest s) => s.GroupId == request.GroupId && s.MemberId == memberId), 
                Arg.Any<CancellationToken>(),
                Arg.Any<IRequestContext>());
#pragma warning restore 4014
        }

        [Test]
        public async Task ConsumerLeaderSyncsGroupWithAssignment()
        {
            var protocol = new JoinGroupRequest.GroupProtocol(new ConsumerProtocolMetadata("mine"));
            var router = Substitute.For<IRouter>();
            var conn = Substitute.For<IConnection>();
            router.GetGroupBrokerAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                  .Returns(_ => Task.FromResult(new GroupBroker(_.Arg<string>(), 0, conn)));            
            router.SyncGroupAsync(Arg.Any<SyncGroupRequest>(), Arg.Any<IRequestContext>(), Arg.Any<IRetry>(), Arg.Any<CancellationToken>())
                .Returns(_ => Task.FromResult(new SyncGroupResponse(ErrorCode.None, new ConsumerMemberAssignment(new [] { new TopicPartition("name", 0) }))));

            var consumer = new Consumer(router);
            var request = new JoinGroupRequest(TestConfig.GroupId(), TimeSpan.FromSeconds(30), "", ConsumerEncoder.Protocol, new [] { protocol });
            var memberId = Guid.NewGuid().ToString("N");
            var response = new JoinGroupResponse(ErrorCode.None, 1, protocol.Name, memberId, memberId, new []{ new JoinGroupResponse.Member(memberId, new ConsumerProtocolMetadata("mine")) });

            using (new ConsumerMember(consumer, request, response, log: TestConfig.Log)) {
                await Task.Delay(300);
            }

#pragma warning disable 4014
            router.Received().SyncGroupAsync(
                Arg.Is((SyncGroupRequest s) => s.GroupId == request.GroupId && s.MemberId == memberId && s.GroupAssignments.Count > 0),
                Arg.Any<IRequestContext>(),
                Arg.Any<IRetry>(), 
                Arg.Any<CancellationToken>());
#pragma warning restore 4014
        }

        [Test]
        public async Task ConsumerFollowerSyncsGroupWithoutAssignment()
        {
            var protocol = new JoinGroupRequest.GroupProtocol(new ConsumerProtocolMetadata("mine"));
            var router = Substitute.For<IRouter>();
            var conn = Substitute.For<IConnection>();
            router.GetGroupBrokerAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                  .Returns(_ => Task.FromResult(new GroupBroker(_.Arg<string>(), 0, conn)));            
            router.SyncGroupAsync(Arg.Any<SyncGroupRequest>(), Arg.Any<IRequestContext>(), Arg.Any<IRetry>(), Arg.Any<CancellationToken>())
                .Returns(_ => Task.FromResult(new SyncGroupResponse(ErrorCode.None, new ConsumerMemberAssignment(new [] { new TopicPartition("name", 0) }))));

            var consumer = new Consumer(router);
            var request = new JoinGroupRequest(TestConfig.GroupId(), TimeSpan.FromSeconds(30), "", ConsumerEncoder.Protocol, new [] { protocol });
            var memberId = Guid.NewGuid().ToString("N");
            var response = new JoinGroupResponse(ErrorCode.None, 1, protocol.Name, "other" + memberId, memberId, new []{ new JoinGroupResponse.Member(memberId, new ConsumerProtocolMetadata("mine")) });

            using (new ConsumerMember(consumer, request, response, TestConfig.Log)) {
                await Task.Delay(300);
            }

#pragma warning disable 4014
            router.Received().SyncGroupAsync(
                Arg.Is((SyncGroupRequest s) => s.GroupId == request.GroupId && s.MemberId == memberId && s.GroupAssignments.Count == 0),
                Arg.Any<IRequestContext>(),
                Arg.Any<IRetry>(), 
                Arg.Any<CancellationToken>());
#pragma warning restore 4014
        }

        [TestCase(0, 100, 0)]
        [TestCase(9, 100, 1000)]
        public async Task ConsumerHeartbeatsAtDesiredIntervals(int expectedHeartbeats, int heartbeatMilliseconds, int totalMilliseconds)
        {
            var protocol = new JoinGroupRequest.GroupProtocol(new ConsumerProtocolMetadata("mine"));
            var router = Substitute.For<IRouter>();
            var conn = Substitute.For<IConnection>();
            router.GetConnectionAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
                  .Returns(_ => Task.FromResult(conn));
            router.SyncGroupAsync(Arg.Any<SyncGroupRequest>(), Arg.Any<IRequestContext>(), Arg.Any<IRetry>(), Arg.Any<CancellationToken>())
                .Returns(_ => Task.FromResult(new SyncGroupResponse(ErrorCode.None, new ConsumerMemberAssignment(new [] { new TopicPartition("name", 0) }))));
            conn.SendAsync(Arg.Any<HeartbeatRequest>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                .Returns(_ => Task.FromResult(new HeartbeatResponse(ErrorCode.None)));

            var consumer = new Consumer(router);
            var request = new JoinGroupRequest(TestConfig.GroupId(), TimeSpan.FromMilliseconds(heartbeatMilliseconds * 2), "", ConsumerEncoder.Protocol, new [] { protocol });
            var memberId = Guid.NewGuid().ToString("N");
            var response = new JoinGroupResponse(ErrorCode.None, 1, protocol.Name, memberId, memberId, new []{ new JoinGroupResponse.Member(memberId, new ConsumerProtocolMetadata("mine")) });

            using (new ConsumerMember(consumer, request, response, TestConfig.Log)) {
                await Task.Delay(totalMilliseconds);
            }

            Assert.That(conn.ReceivedCalls()
                            .Count(c => {
                                if (c.GetMethodInfo().Name != nameof(Connection.SendAsync)) return false;
                                var s = c.GetArguments()[0] as HeartbeatRequest;
                                if (s == null) return false;
                                return s.GroupId == request.GroupId && s.MemberId == memberId && s.GenerationId == response.GenerationId;
                            }), Is.InRange(expectedHeartbeats - 1, expectedHeartbeats + 1));
        }

        [TestCase(100, 700)]
        [TestCase(150, 700)]
        [TestCase(250, 700)]
        public async Task ConsumerHeartbeatsWithinTimeLimit(int heartbeatMilliseconds, int totalMilliseconds)
        {
            var protocol = new JoinGroupRequest.GroupProtocol(new ConsumerProtocolMetadata("mine"));
            var router = Substitute.For<IRouter>();
            var conn = Substitute.For<IConnection>();
            router.GetGroupBrokerAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                  .Returns(_ => Task.FromResult(new GroupBroker(_.Arg<string>(), 0, conn)));

            var consumer = new Consumer(router);
            var lastHeartbeat = DateTimeOffset.UtcNow;
            var heartbeatIntervals = ImmutableArray<TimeSpan>.Empty;
            conn.SendAsync(Arg.Any<HeartbeatRequest>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                .Returns(_ => {
                            heartbeatIntervals = heartbeatIntervals.Add(DateTimeOffset.UtcNow - lastHeartbeat);
                            lastHeartbeat = DateTimeOffset.UtcNow;
                            return Task.FromResult(new HeartbeatResponse(ErrorCode.None));
                        });
            var request = new JoinGroupRequest(TestConfig.GroupId(), TimeSpan.FromMilliseconds(heartbeatMilliseconds), "", ConsumerEncoder.Protocol, new [] { protocol });
            var memberId = Guid.NewGuid().ToString("N");
            var response = new JoinGroupResponse(ErrorCode.None, 1, protocol.Name, memberId, memberId, new []{ new JoinGroupResponse.Member(memberId, new ConsumerProtocolMetadata("mine")) });
            lastHeartbeat = DateTimeOffset.UtcNow;

            using (new ConsumerMember(consumer, request, response, TestConfig.Log)) {
                await Task.Delay(totalMilliseconds);
            }

            foreach (var interval in heartbeatIntervals) {
                Assert.That((int)interval.TotalMilliseconds, Is.AtMost(heartbeatMilliseconds));
            }
        }

        [TestCase(100)]
        [TestCase(150)]
        [TestCase(250)]
        public async Task ConsumerHeartbeatsUntilDisposed(int heartbeatMilliseconds)
        {
            var protocol = new JoinGroupRequest.GroupProtocol(new ConsumerProtocolMetadata("mine"));
            var router = Substitute.For<IRouter>();
            var conn = Substitute.For<IConnection>();
            router.GetConnectionAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
                  .Returns(_ => Task.FromResult(conn));
            router.SyncGroupAsync(Arg.Any<SyncGroupRequest>(), Arg.Any<IRequestContext>(), Arg.Any<IRetry>(), Arg.Any<CancellationToken>())
                .Returns(_ => Task.FromResult(new SyncGroupResponse(ErrorCode.None, new ConsumerMemberAssignment(new [] { new TopicPartition("name", 0) }))));
            conn.SendAsync(Arg.Any<HeartbeatRequest>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                .Returns(_ => Task.FromResult(new HeartbeatResponse(ErrorCode.NetworkException)));

            var consumer = new Consumer(router);
            var request = new JoinGroupRequest(TestConfig.GroupId(), TimeSpan.FromMilliseconds(heartbeatMilliseconds), "", ConsumerEncoder.Protocol, new [] { protocol });
            var memberId = Guid.NewGuid().ToString("N");
            var response = new JoinGroupResponse(ErrorCode.None, 1, protocol.Name, memberId, memberId, new []{ new JoinGroupResponse.Member(memberId, new ConsumerProtocolMetadata("mine")) });

            using (new ConsumerMember(consumer, request, response, log: TestConfig.Log)) {
                await Task.Delay(heartbeatMilliseconds * 3);

#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                conn.DidNotReceive().SendAsync(
                    Arg.Is((LeaveGroupRequest s) => s.GroupId == request.GroupId && s.MemberId == memberId),
                    Arg.Any<CancellationToken>(),
                    Arg.Any<IRequestContext>());
            }
            conn.Received().SendAsync(
                Arg.Is((LeaveGroupRequest s) => s.GroupId == request.GroupId && s.MemberId == memberId),
                Arg.Any<CancellationToken>(),
                Arg.Any<IRequestContext>());
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed

            Assert.That(conn.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(Connection.SendAsync) && (c.GetArguments()[0] as HeartbeatRequest) != null), Is.AtLeast(2));
        }

        // design unit TESTS to write:
        // (async) locking is done correctly in the member
        // dealing correctly with losing ownership
        // multiple partition assignment test
        // initial group describe dictates what call happens next (based on server state)
        // add router tests for group metadata caching
    }
}