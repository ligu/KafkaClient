using System;
using System.Net;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Connections;
using KafkaClient.Protocol;
using NSubstitute;

namespace KafkaClient.Tests
{
    public class RoutingScenario
    {
        public const string TestTopic = "UnitTest";

        private int _offset1;
        public FakeConnection Connection1 { get; }

        private int _offset2;
        public FakeConnection Connection2 { get; }

        public IConnectionFactory KafkaConnectionFactory { get; }

        public Func<Task<MetadataResponse>> MetadataResponse = DefaultMetadataResponse;
        public Func<Task<GroupCoordinatorResponse>> GroupCoordinatorResponse = () => DefaultGroupCoordinatorResponse(0);

        public RoutingScenario()
        {
            //setup mock IConnection
#pragma warning disable 1998
            Connection1 = new FakeConnection(new Endpoint(new IPEndPoint(IPAddress.Loopback, 1))) {
                { ApiKey.Produce, async _ => new ProduceResponse(new ProduceResponse.Topic(TestTopic, 0, ErrorCode.None, _offset1++)) },
                { ApiKey.Metadata, async _ => await MetadataResponse() },
                { ApiKey.GroupCoordinator, async _ => await GroupCoordinatorResponse() },
                { ApiKey.Offset, async _ => new OffsetResponse(
                    new[] {
                        new OffsetResponse.Topic(TestTopic, 0, ErrorCode.None, 0L),
                        new OffsetResponse.Topic(TestTopic, 0, ErrorCode.None, 99L)
                    }) },
                { ApiKey.Fetch, async _ => {
                        await Task.Delay(500);
                        return null;
                    }
                }
            };

            Connection2 = new FakeConnection(new Endpoint(new IPEndPoint(IPAddress.Loopback, 2))) {
                { ApiKey.Produce, async _ => new ProduceResponse(new ProduceResponse.Topic(TestTopic, 1, ErrorCode.None, _offset2++)) },
                { ApiKey.Metadata, async _ => await MetadataResponse() },
                { ApiKey.GroupCoordinator, async _ => await GroupCoordinatorResponse() },
                { ApiKey.Offset, async _ => new OffsetResponse(
                    new[] {
                        new OffsetResponse.Topic(TestTopic, 1, ErrorCode.None, 0L),
                        new OffsetResponse.Topic(TestTopic, 1, ErrorCode.None, 100L)
                    }) },
                { ApiKey.Fetch, async _ => {
                        await Task.Delay(500);
                        return null;
                    }
                }
            };
#pragma warning restore 1998

            var kafkaConnectionFactory = Substitute.For<IConnectionFactory>();
            kafkaConnectionFactory
                .Create(Arg.Is<Endpoint>(e => e.Ip.Port == 1), Arg.Any<IConnectionConfiguration>(), Arg.Any<ILog>())
                .Returns(Connection1);
            kafkaConnectionFactory
                .Create(Arg.Is<Endpoint>(e => e.Ip.Port == 2), Arg.Any<IConnectionConfiguration>(), Arg.Any<ILog>())
                .Returns(Connection2);
            KafkaConnectionFactory = kafkaConnectionFactory;
        }

        public IRouter CreateRouter(TimeSpan? cacheExpiration = null)
        {
            return new Router(
                new [] { new Endpoint(new IPEndPoint(IPAddress.Loopback, 1)), new Endpoint(new IPEndPoint(IPAddress.Loopback, 2)) },
                KafkaConnectionFactory,
                routerConfiguration: new RouterConfiguration(cacheExpiration: cacheExpiration.GetValueOrDefault(TimeSpan.FromMinutes(1))),
                log: TestConfig.Log);
        }

#pragma warning disable 1998
        public static async Task<MetadataResponse> DefaultMetadataResponse()
        {
            return new MetadataResponse(
                new [] {
                    new Protocol.Server(0, "localhost", 1),
                    new Protocol.Server(1, "localhost", 2)
                },
                new [] {
                    new MetadataResponse.Topic(TestTopic, 
                        ErrorCode.None, new [] {
                            new MetadataResponse.Partition(0, 0, ErrorCode.None, new [] { 1 }, new []{ 1 }),
                            new MetadataResponse.Partition(1, 1, ErrorCode.None, new [] { 1 }, new []{ 1 }),
                        })
                });
        }

        public static async Task<GroupCoordinatorResponse> DefaultGroupCoordinatorResponse(int brokerId)
        {
            return new GroupCoordinatorResponse(ErrorCode.None, 0, "localhost", brokerId + 1);
        }

        public static async Task<MetadataResponse> MetadataResponseWithSingleBroker()
        {
            return new MetadataResponse(
                new [] {
                    new Protocol.Server(1, "localhost", 2)
                },
                new [] {
                    new MetadataResponse.Topic(TestTopic, 
                        ErrorCode.None, new [] {
                            new MetadataResponse.Partition(0, 1, ErrorCode.None, new [] { 1 }, new []{ 1 }),
                            new MetadataResponse.Partition(1, 1, ErrorCode.None, new [] { 1 }, new []{ 1 }),
                        })
                });
        }

        public static async Task<MetadataResponse> MetadataResponseWithNotEndToElectLeader()
        {
            return new MetadataResponse(
                new [] {
                    new Protocol.Server(1, "localhost", 2)
                },
                new [] {
                    new MetadataResponse.Topic(TestTopic, 
                        ErrorCode.None, new [] {
                            new MetadataResponse.Partition(0, -1, ErrorCode.None, new [] { 1 }, new []{ 1 }),
                            new MetadataResponse.Partition(1, 1, ErrorCode.None, new [] { 1 }, new []{ 1 }),
                        })
                });
        }

        public static async Task<MetadataResponse> MetaResponseWithException()
        {
            throw new Exception();
        }
#pragma warning restore 1998

    }
}