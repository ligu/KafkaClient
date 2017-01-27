using System;
using System.Net;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Connections;
using KafkaClient.Protocol;
using NSubstitute;

namespace KafkaClient.Tests
{
    public class FakeBrokerRouter
    {
        public const string TestTopic = "UnitTest";

        private int _offset0;
        private int _offset1;
        private readonly FakeConnection _fakeConn0;
        private readonly FakeConnection _fakeConn1;
        private readonly IConnectionFactory _mockConnectionFactory;
        public readonly TimeSpan _cacheExpiration = TimeSpan.FromMilliseconds(1);
        public FakeConnection BrokerConn0 { get { return _fakeConn0; } }
        public FakeConnection BrokerConn1 { get { return _fakeConn1; } }
        public IConnectionFactory ConnectionMockConnectionFactory { get { return _mockConnectionFactory; } }

        public Func<Task<IResponse>> MetadataResponse = DefaultMetadataResponse;

        public FakeBrokerRouter()
        {
            //setup mock IConnection

#pragma warning disable 1998
            _fakeConn0 = new FakeConnection(new Uri("http://localhost:1")) {
                { ApiKey.Produce, async _ => new ProduceResponse(new ProduceResponse.Topic(TestTopic, 0, ErrorCode.None, _offset0++)) },
                { ApiKey.Metadata, _ => MetadataResponse() },
                { ApiKey.Offset, async _ => new OffsetResponse(new [] {
                    new OffsetResponse.Topic(TestTopic, 0, ErrorCode.None, 0L),
                    new OffsetResponse.Topic(TestTopic, 0, ErrorCode.None, 99L)
                }) },
                { ApiKey.Fetch, async _ => {
                        await Task.Delay(500);
                        return null;
                    }
                }
            };

            _fakeConn1 = new FakeConnection(new Uri("http://localhost:2")) {
                { ApiKey.Produce, async _ => new ProduceResponse(new ProduceResponse.Topic(TestTopic, 1, ErrorCode.None, _offset1++)) },
                { ApiKey.Metadata, _ => MetadataResponse() },
                { ApiKey.Offset, async _ => new OffsetResponse(new [] {
                    new OffsetResponse.Topic(TestTopic, 0, ErrorCode.None, 0L),
                    new OffsetResponse.Topic(TestTopic, 0, ErrorCode.None, 100L)
                }) },
                { ApiKey.Fetch, async _ => {
                        await Task.Delay(500);
                        return null;
                    }
                }
            };
#pragma warning restore 1998

            _mockConnectionFactory = Substitute.For<IConnectionFactory>();
            _mockConnectionFactory.Create(Arg.Is<Endpoint>(e => e.Value.Port == 1), Arg.Any<IConnectionConfiguration>(), Arg.Any<ILog>()).Returns(_fakeConn0);
            _mockConnectionFactory.Create(Arg.Is<Endpoint>(e => e.Value.Port == 2), Arg.Any<IConnectionConfiguration>(), Arg.Any<ILog>()).Returns(_fakeConn1);
            _mockConnectionFactory.ResolveAsync(Arg.Any<Uri>(), Arg.Any<ILog>())
                                  .Returns(info => Task.FromResult(new Endpoint(new IPEndPoint(IPAddress.Loopback, ((Uri)info[0]).Port), ((Uri)info[0]).DnsSafeHost)));
        }

        public IRouter Create()
        {
            return new Router(
                new [] { new Endpoint(new IPEndPoint(IPAddress.Loopback, 1)), new Endpoint(new IPEndPoint(IPAddress.Loopback, 2)) },
                _mockConnectionFactory,
                routerConfiguration: new RouterConfiguration(cacheExpiration: _cacheExpiration));
        }

#pragma warning disable 1998
        public static async Task<IResponse> DefaultMetadataResponse()
        {
            return new MetadataResponse(
                new [] {
                    new KafkaClient.Protocol.Broker(0, "localhost", 1),
                    new KafkaClient.Protocol.Broker(1, "localhost", 2)
                },
                new [] {
                    new MetadataResponse.Topic(TestTopic, 
                        ErrorCode.None, new [] {
                                          new MetadataResponse.Partition(0, 0, ErrorCode.None, new [] { 1 }, new []{ 1 }),
                                          new MetadataResponse.Partition(1, 1, ErrorCode.None, new [] { 1 }, new []{ 1 }),
                                      })
                });
        }
#pragma warning restore 1998
    }
}