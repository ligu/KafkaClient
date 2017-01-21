using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Connections;
using KafkaClient.Protocol;
using NSubstitute;

namespace KafkaClient.Tests.Fakes
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

        public IPartitionSelector PartitionSelector = new PartitionSelector();

        public BrokerRouterProxy()
        {
            //setup mock IConnection
            Connection1 = new FakeConnection(new Uri("http://localhost:1"));
#pragma warning disable 1998
            Connection1.ProduceResponseFunction = async () => new ProduceResponse(new ProduceResponse.Topic(TestTopic, 0, ErrorResponseCode.None, _offset1++));
            Connection1.MetadataResponseFunction = () => MetadataResponse();
            Connection1.OffsetResponseFunction = async () => new OffsetResponse(new [] {
                new OffsetResponse.Topic(TestTopic, 0, ErrorResponseCode.None, 0L),
                new OffsetResponse.Topic(TestTopic, 0, ErrorResponseCode.None, 99L)
            });
            Connection1.FetchResponseFunction = async () => { Thread.Sleep(500); return null; };

            Connection2 = new FakeConnection(new Uri("http://localhost:2"));
            Connection2.ProduceResponseFunction = async () => new ProduceResponse(new ProduceResponse.Topic(TestTopic, 1, ErrorResponseCode.None, _offset2++));
            Connection2.MetadataResponseFunction = () => MetadataResponse();
            Connection2.OffsetResponseFunction = async () => new OffsetResponse(new [] {
                new OffsetResponse.Topic(TestTopic, 1, ErrorResponseCode.None, 0L),
                new OffsetResponse.Topic(TestTopic, 1, ErrorResponseCode.None, 100L)
            });
            Connection2.FetchResponseFunction = async () => { Thread.Sleep(500); return null; };
#pragma warning restore 1998

            var kafkaConnectionFactory = Substitute.For<IConnectionFactory>();
            kafkaConnectionFactory
                .Create(Arg.Is<Endpoint>(e => e.IP.Port == 1), Arg.Any<IConnectionConfiguration>(), Arg.Any<ILog>())
                .Returns(Connection1);
            kafkaConnectionFactory
                .Create(Arg.Is<Endpoint>(e => e.IP.Port == 2), Arg.Any<IConnectionConfiguration>(), Arg.Any<ILog>())
                .Returns(Connection2);
            kafkaConnectionFactory
                .Resolve(Arg.Any<Uri>(), Arg.Any<ILog>())
                .Returns(_ => new Endpoint(_.Arg<Uri>(), new IPEndPoint(IPAddress.Parse("127.0.0.1"), _.Arg<Uri>().Port)));
            KafkaConnectionFactory = kafkaConnectionFactory;
        }

        public IBrokerRouter Create()
        {
            return new BrokerRouter(
                new [] { new Uri("http://localhost:1"), new Uri("http://localhost:2") },
                KafkaConnectionFactory,
                partitionSelector: PartitionSelector,
                cacheConfiguration: new CacheConfiguration(cacheExpiration: CacheExpiration));
        }

#pragma warning disable 1998
        public static async Task<MetadataResponse> CreateMetadataResponseWithMultipleBrokers()
        {
            return new MetadataResponse(
                new [] {
                    new Broker(0, "localhost", 1),
                    new Broker(1, "localhost", 2)
                },
                new [] {
                    new MetadataResponse.Topic(TestTopic, 
                        ErrorResponseCode.None, new [] {
                            new MetadataResponse.Partition(0, 0, ErrorResponseCode.None, new [] { 1 }, new []{ 1 }),
                            new MetadataResponse.Partition(1, 1, ErrorResponseCode.None, new [] { 1 }, new []{ 1 }),
                        })
                });
        }


        public static async Task<MetadataResponse> CreateMetadataResponseWithSingleBroker()
        {
            return new MetadataResponse(
                new [] {
                    new Broker(1, "localhost", 2)
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
                    new Broker(1, "localhost", 2)
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