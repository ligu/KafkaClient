using System;
using System.Net;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Connections;
using KafkaClient.Protocol;
using NSubstitute;

namespace KafkaClient.Tests
{
    public class BrokerRouterProxy
    {
        public const string TestTopic = "UnitTest";

        public TimeSpan CacheExpiration = TimeSpan.FromMilliseconds(1);

        private int _offset1;
        public FakeConnection Connection1 { get; }

        private int _offset2;
        public FakeConnection Connection2 { get; }

        public IConnectionFactory KafkaConnectionFactory { get; }

        public Func<Task<MetadataResponse>> MetadataResponse = CreateMetadataResponseWithMultipleBrokers;
        public Func<Task<GroupCoordinatorResponse>> GroupCoordinatorResponse = () => CreateGroupCoordinatorResponse(0);

        public BrokerRouterProxy()
        {
            //setup mock IConnection
#pragma warning disable 1998
            Connection1 = new FakeConnection(new Uri("http://localhost:1")) {
                { ApiKeyRequestType.Produce, async _ => new ProduceResponse(new ProduceResponse.Topic(TestTopic, 0, ErrorResponseCode.None, _offset1++)) },
                { ApiKeyRequestType.Metadata, async _ => await MetadataResponse() },
                { ApiKeyRequestType.GroupCoordinator, async _ => await GroupCoordinatorResponse() },
                { ApiKeyRequestType.Offset, async _ => new OffsetResponse(
                    new[] {
                        new OffsetResponse.Topic(TestTopic, 0, ErrorResponseCode.None, 0L),
                        new OffsetResponse.Topic(TestTopic, 0, ErrorResponseCode.None, 99L)
                    }) },
                { ApiKeyRequestType.Fetch, async _ => {
                        await Task.Delay(500);
                        return null;
                    }
                }
            };

            Connection2 = new FakeConnection(new Uri("http://localhost:2")) {
                { ApiKeyRequestType.Produce, async _ => new ProduceResponse(new ProduceResponse.Topic(TestTopic, 1, ErrorResponseCode.None, _offset2++)) },
                { ApiKeyRequestType.Metadata, async _ => await MetadataResponse() },
                { ApiKeyRequestType.GroupCoordinator, async _ => await GroupCoordinatorResponse() },
                { ApiKeyRequestType.Offset, async _ => new OffsetResponse(
                    new[] {
                        new OffsetResponse.Topic(TestTopic, 1, ErrorResponseCode.None, 0L),
                        new OffsetResponse.Topic(TestTopic, 1, ErrorResponseCode.None, 100L)
                    }) },
                { ApiKeyRequestType.Fetch, async _ => {
                        await Task.Delay(500);
                        return null;
                    }
                }
            };
#pragma warning restore 1998

            var kafkaConnectionFactory = Substitute.For<IConnectionFactory>();
            kafkaConnectionFactory
                .Create(Arg.Is<Endpoint>(e => e.Value.Port == 1), Arg.Any<IConnectionConfiguration>(),Arg.Any<ILog>())
                .Returns(Connection1);
            kafkaConnectionFactory
                .Create(Arg.Is<Endpoint>(e => e.Value.Port == 2), Arg.Any<IConnectionConfiguration>(),Arg.Any<ILog>())
                .Returns(Connection2);
            kafkaConnectionFactory
                .ResolveAsync(Arg.Any<Uri>(), Arg.Any<ILog>())
                .Returns(_ => Task.FromResult(new Endpoint(new IPEndPoint(IPAddress.Parse("127.0.0.1"), _.Arg<Uri>().Port), _.Arg<Uri>().DnsSafeHost)));
            KafkaConnectionFactory = kafkaConnectionFactory;
        }

        public IRouter Create()
        {
            return new Router(
                new [] { new Endpoint(new IPEndPoint(IPAddress.Loopback, 1)), new Endpoint(new IPEndPoint(IPAddress.Loopback, 2)) },
                KafkaConnectionFactory,
                routerConfiguration: new RouterConfiguration(cacheExpiration: CacheExpiration));
        }

#pragma warning disable 1998
        public static async Task<MetadataResponse> CreateMetadataResponseWithMultipleBrokers()
        {
            return new MetadataResponse(
                new [] {
                    new KafkaClient.Protocol.Broker(0, "localhost", 1),
                    new KafkaClient.Protocol.Broker(1, "localhost", 2)
                },
                new [] {
                    new MetadataResponse.Topic(TestTopic, 
                        ErrorResponseCode.None, new [] {
                            new MetadataResponse.Partition(0, 0, ErrorResponseCode.None, new [] { 1 }, new []{ 1 }),
                            new MetadataResponse.Partition(1, 1, ErrorResponseCode.None, new [] { 1 }, new []{ 1 }),
                        })
                });
        }

        public static async Task<GroupCoordinatorResponse> CreateGroupCoordinatorResponse(int brokerId)
        {
            return new GroupCoordinatorResponse(ErrorResponseCode.None, 0, "localhost", brokerId + 1);
        }

        public static async Task<MetadataResponse> CreateMetadataResponseWithSingleBroker()
        {
            return new MetadataResponse(
                new [] {
                    new KafkaClient.Protocol.Broker(1, "localhost", 2)
                },
                new [] {
                    new MetadataResponse.Topic(TestTopic, 
                        ErrorResponseCode.None, new [] {
                            new MetadataResponse.Partition(0, 1, ErrorResponseCode.None, new [] { 1 }, new []{ 1 }),
                            new MetadataResponse.Partition(1, 1, ErrorResponseCode.None, new [] { 1 }, new []{ 1 }),
                        })
                });
        }

        public static async Task<MetadataResponse> CreateMetadataResponseWithNotEndToElectLeader()
        {
            return new MetadataResponse(
                new [] {
                    new KafkaClient.Protocol.Broker(1, "localhost", 2)
                },
                new [] {
                    new MetadataResponse.Topic(TestTopic, 
                        ErrorResponseCode.None, new [] {
                            new MetadataResponse.Partition(0, -1, ErrorResponseCode.None, new [] { 1 }, new []{ 1 }),
                            new MetadataResponse.Partition(1, 1, ErrorResponseCode.None, new [] { 1 }, new []{ 1 }),
                        })
                });
        }

        public static async Task<MetadataResponse> CreateMetaResponseWithException()
        {
            throw new Exception();
        }
#pragma warning restore 1998

    }
}