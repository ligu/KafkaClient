using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Connection;
using KafkaClient.Protocol;
using Moq;
using Ninject.MockingKernel.Moq;

namespace KafkaClient.Tests.Fakes
{
    public class BrokerRouterProxy
    {
        public const string TestTopic = "UnitTest";

        private readonly MoqMockingKernel _kernel;
        private int _offset0;
        private int _offset1;
        private readonly FakeConnection _fakeConn0;
        private readonly FakeConnection _fakeConn1;
        private readonly Mock<IConnectionFactory> _mockKafkaConnectionFactory;
        public TimeSpan _cacheExpiration = TimeSpan.FromMilliseconds(1);
        public FakeConnection BrokerConn0 { get { return _fakeConn0; } }
        public FakeConnection BrokerConn1 { get { return _fakeConn1; } }
        public Mock<IConnectionFactory> KafkaConnectionMockKafkaConnectionFactory { get { return _mockKafkaConnectionFactory; } }

        public Func<Task<MetadataResponse>> MetadataResponse = CreateMetadataResponseWithMultipleBrokers;

        public IPartitionSelector PartitionSelector = new PartitionSelector();

        public BrokerRouterProxy(MoqMockingKernel kernel)
        {
            _kernel = kernel;

            //setup mock IConnection
            _fakeConn0 = new FakeConnection(new Uri("http://localhost:1"));
#pragma warning disable 1998
            _fakeConn0.ProduceResponseFunction = async () => new ProduceResponse(new ProduceTopic(TestTopic, 0, ErrorResponseCode.NoError, _offset0++));
            _fakeConn0.MetadataResponseFunction = () => MetadataResponse();
            _fakeConn0.OffsetResponseFunction = async () => new OffsetResponse(new OffsetTopic(TestTopic, 0, ErrorResponseCode.NoError, new []{ 0L, 99L }));
            _fakeConn0.FetchResponseFunction = async () => { Thread.Sleep(500); return null; };

            _fakeConn1 = new FakeConnection(new Uri("http://localhost:2"));
            _fakeConn1.ProduceResponseFunction = async () => new ProduceResponse(new ProduceTopic(TestTopic, 1, ErrorResponseCode.NoError, _offset1++));
            _fakeConn1.MetadataResponseFunction = () => MetadataResponse();
            _fakeConn1.OffsetResponseFunction = async () => new OffsetResponse(new OffsetTopic(TestTopic, 1, ErrorResponseCode.NoError, new []{ 0L, 100L }));
            _fakeConn1.FetchResponseFunction = async () => { Thread.Sleep(500); return null; };
#pragma warning restore 1998

            _mockKafkaConnectionFactory = _kernel.GetMock<IConnectionFactory>();
            _mockKafkaConnectionFactory.Setup(x => x.Create(It.Is<Endpoint>(e => e.IP.Port == 1), It.IsAny<IConnectionConfiguration>(), It.IsAny<ILog>())).Returns(() => _fakeConn0);
            _mockKafkaConnectionFactory.Setup(x => x.Create(It.Is<Endpoint>(e => e.IP.Port == 2), It.IsAny<IConnectionConfiguration>(), It.IsAny<ILog>())).Returns(() => _fakeConn1);
            _mockKafkaConnectionFactory.Setup(x => x.Resolve(It.IsAny<Uri>(), It.IsAny<ILog>()))
                .Returns<Uri, ILog>((uri, log) => new Endpoint(uri, new IPEndPoint(IPAddress.Parse("127.0.0.1"), uri.Port)));
        }

        public IBrokerRouter Create()
        {
            return new BrokerRouter(
                new [] { new Uri("http://localhost:1"), new Uri("http://localhost:2") },
                _mockKafkaConnectionFactory.Object,
                partitionSelector: PartitionSelector,
                cacheConfiguration: new CacheConfiguration(cacheExpiration: _cacheExpiration));
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