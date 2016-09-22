using System;
using System.Net;
using System.Threading;
using KafkaClient.Common;
using KafkaClient.Connection;
using KafkaClient.Protocol;
using NSubstitute;

namespace KafkaClient.Tests.Fakes
{
    public class FakeBrokerRouter
    {
        public const string TestTopic = "UnitTest";

        private int _offset0;
        private int _offset1;
        private readonly FakeKafkaConnection _fakeConn0;
        private readonly FakeKafkaConnection _fakeConn1;
        private readonly IKafkaConnectionFactory _mockKafkaConnectionFactory;
        public readonly TimeSpan _cacheExpiration = TimeSpan.FromMilliseconds(1);
        public FakeKafkaConnection BrokerConn0 { get { return _fakeConn0; } }
        public FakeKafkaConnection BrokerConn1 { get { return _fakeConn1; } }
        public IKafkaConnectionFactory KafkaConnectionMockKafkaConnectionFactory { get { return _mockKafkaConnectionFactory; } }

        public Func<MetadataResponse> MetadataResponse = () => DefaultMetadataResponse();

        public IPartitionSelector PartitionSelector = new PartitionSelector();

        public FakeBrokerRouter()
        {
            //setup mock IKafkaConnection

            _fakeConn0 = new FakeKafkaConnection(new Uri("http://localhost:1"));
#pragma warning disable 1998
            _fakeConn0.ProduceResponseFunction = async () => new ProduceResponse(new ProduceTopic(TestTopic, 0, ErrorResponseCode.NoError, _offset0++));
            _fakeConn0.MetadataResponseFunction = async () => MetadataResponse();
            _fakeConn0.OffsetResponseFunction = async () => new OffsetResponse(new OffsetTopic(TestTopic, 0, ErrorResponseCode.NoError, new []{ 0L, 99L }));
            _fakeConn0.FetchResponseFunction = async () => { Thread.Sleep(500); return null; };

            _fakeConn1 = new FakeKafkaConnection(new Uri("http://localhost:2"));
            _fakeConn1.ProduceResponseFunction = async () => new ProduceResponse(new ProduceTopic(TestTopic, 1, ErrorResponseCode.NoError, _offset1++));
            _fakeConn1.MetadataResponseFunction = async () => MetadataResponse();
            _fakeConn1.OffsetResponseFunction = async () => new OffsetResponse(new OffsetTopic(TestTopic, 1, ErrorResponseCode.NoError, new []{ 0L, 100L }));
            _fakeConn1.FetchResponseFunction = async () => { Thread.Sleep(500); return null; };
#pragma warning restore 1998

            _mockKafkaConnectionFactory = Substitute.For<IKafkaConnectionFactory>();
            _mockKafkaConnectionFactory.Create(Arg.Is<KafkaEndpoint>(e => e.Endpoint.Port == 1), Arg.Any<TimeSpan>(), Arg.Any<IKafkaLog>(), Arg.Any<int>(), Arg.Any<TimeSpan?>(), Arg.Any<bool>()).Returns(_fakeConn0);
            _mockKafkaConnectionFactory.Create(Arg.Is<KafkaEndpoint>(e => e.Endpoint.Port == 2), Arg.Any<TimeSpan>(), Arg.Any<IKafkaLog>(), Arg.Any<int>(), Arg.Any<TimeSpan?>(), Arg.Any<bool>()).Returns(_fakeConn1);
            _mockKafkaConnectionFactory.Resolve(Arg.Any<Uri>(), Arg.Any<IKafkaLog>())
                                       .Returns(info => new KafkaEndpoint((Uri)info[0], new IPEndPoint(IPAddress.Parse("127.0.0.1"), ((Uri)info[0]).Port)));
        }

        public IBrokerRouter Create()
        {
            return new BrokerRouter(
                new [] { new Uri("http://localhost:1"), new Uri("http://localhost:2") },
                _mockKafkaConnectionFactory,
                partitionSelector: PartitionSelector,
                cacheOptions: new MetadataCacheOptions(cacheExpiration: _cacheExpiration));
        }

        public static MetadataResponse DefaultMetadataResponse()
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
    }
}