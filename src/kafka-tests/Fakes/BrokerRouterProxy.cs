using kafka_tests.Fakes;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using Moq;
using Ninject.MockingKernel.Moq;
using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace kafka_tests
{
    public class BrokerRouterProxy
    {
        public const string TestTopic = "UnitTest";

        private readonly MoqMockingKernel _kernel;
        private int _offset0;
        private int _offset1;
        private readonly FakeKafkaConnection _fakeConn0;
        private readonly FakeKafkaConnection _fakeConn1;
        private readonly Mock<IKafkaConnectionFactory> _mockKafkaConnectionFactory;
        public TimeSpan _cacheExpiration = TimeSpan.FromMilliseconds(10);
        public FakeKafkaConnection BrokerConn0 { get { return _fakeConn0; } }
        public FakeKafkaConnection BrokerConn1 { get { return _fakeConn1; } }
        public Mock<IKafkaConnectionFactory> KafkaConnectionMockKafkaConnectionFactory { get { return _mockKafkaConnectionFactory; } }

        public Func<Task<MetadataResponse>> MetadataResponse = CreateMetadataResponseWithMultipleBrokers;

        public IPartitionSelector PartitionSelector = new DefaultPartitionSelector();

        public BrokerRouterProxy(MoqMockingKernel kernel)
        {
            _kernel = kernel;

            //setup mock IKafkaConnection
            _fakeConn0 = new FakeKafkaConnection(new Uri("http://localhost:1"));
#pragma warning disable 1998
            _fakeConn0.ProduceResponseFunction = async () => new ProduceResponse(0, new []{ new ProduceTopic(TestTopic, 0, ErrorResponseCode.NoError, _offset0++)});
            _fakeConn0.MetadataResponseFunction = () => MetadataResponse();
            _fakeConn0.OffsetResponseFunction = async () => new OffsetTopic(TestTopic, 0, ErrorResponseCode.NoError, new []{ 0L, 99L });
            _fakeConn0.FetchResponseFunction = async () => { Thread.Sleep(500); return null; };

            _fakeConn1 = new FakeKafkaConnection(new Uri("http://localhost:2"));
            _fakeConn1.ProduceResponseFunction = async () => new ProduceResponse(0, new []{ new ProduceTopic(TestTopic, 1, ErrorResponseCode.NoError, _offset1++)});
            _fakeConn1.MetadataResponseFunction = () => MetadataResponse();
            _fakeConn1.OffsetResponseFunction = async () => new OffsetTopic(TestTopic, 1, ErrorResponseCode.NoError, new []{ 0L, 100L });
            _fakeConn1.FetchResponseFunction = async () => { Thread.Sleep(500); return null; };
#pragma warning restore 1998

            _mockKafkaConnectionFactory = _kernel.GetMock<IKafkaConnectionFactory>();
            _mockKafkaConnectionFactory.Setup(x => x.Create(It.Is<KafkaEndpoint>(e => e.Endpoint.Port == 1), It.IsAny<TimeSpan>(), It.IsAny<IKafkaLog>(), It.IsAny<int>(), It.IsAny<TimeSpan?>(), It.IsAny<StatisticsTrackerOptions>())).Returns(() => _fakeConn0);
            _mockKafkaConnectionFactory.Setup(x => x.Create(It.Is<KafkaEndpoint>(e => e.Endpoint.Port == 2), It.IsAny<TimeSpan>(), It.IsAny<IKafkaLog>(), It.IsAny<int>(), It.IsAny<TimeSpan?>(), It.IsAny<StatisticsTrackerOptions>())).Returns(() => _fakeConn1);
            _mockKafkaConnectionFactory.Setup(x => x.Resolve(It.IsAny<Uri>(), It.IsAny<IKafkaLog>()))
                .Returns<Uri, IKafkaLog>((uri, log) => new KafkaEndpoint(uri, new IPEndPoint(IPAddress.Parse("127.0.0.1"), uri.Port)));
        }

        public IBrokerRouter Create()
        {
            return new BrokerRouter(new KafkaNet.Model.KafkaOptions
            {
                KafkaServerUri = new List<Uri> { new Uri("http://localhost:1"), new Uri("http://localhost:2") },
                CacheExpiration = _cacheExpiration,
                KafkaConnectionFactory = _mockKafkaConnectionFactory.Object,
                PartitionSelector = PartitionSelector
            });
        }

#pragma warning disable 1998
        public static async Task<MetadataResponse> CreateMetadataResponseWithMultipleBrokers()
        {
            return new MetadataResponse(
                1, 
                new [] {
                    new Broker(0, "localhost", 1),
                    new Broker(1, "localhost", 2)
                },
                new [] {
                    new MetadataTopic(TestTopic, 
                        ErrorResponseCode.NoError, new [] {
                                          new MetadataPartition(0, 0, ErrorResponseCode.NoError, new [] { 1 }, new []{ 1 }),
                                          new MetadataPartition(1, 1, ErrorResponseCode.NoError, new [] { 1 }, new []{ 1 }),
                                      })
                });
        }


        public static async Task<MetadataResponse> CreateMetadataResponseWithSingleBroker()
        {
            return new MetadataResponse(
                1, 
                new [] {
                    new Broker(1, "localhost", 2)
                },
                new [] {
                    new MetadataTopic(TestTopic, 
                        ErrorResponseCode.NoError, new [] {
                                          new MetadataPartition(0, 1, ErrorResponseCode.NoError, new [] { 1 }, new []{ 1 }),
                                          new MetadataPartition(1, 1, ErrorResponseCode.NoError, new [] { 1 }, new []{ 1 }),
                                      })
                });
        }

        public static async Task<MetadataResponse> CreateMetadataResponseWithNotEndToElectLeader()
        {
            return new MetadataResponse(
                1, 
                new [] {
                    new Broker(1, "localhost", 2)
                },
                new [] {
                    new MetadataTopic(TestTopic, 
                        ErrorResponseCode.NoError, new [] {
                                          new MetadataPartition(0, -1, ErrorResponseCode.NoError, new [] { 1 }, new []{ 1 }),
                                          new MetadataPartition(1, 1, ErrorResponseCode.NoError, new [] { 1 }, new []{ 1 }),
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