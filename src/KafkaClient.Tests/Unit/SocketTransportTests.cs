using System;
using KafkaClient.Common;
using KafkaClient.Connections;
using NUnit.Framework;

namespace KafkaClient.Tests.Unit
{
    [TestFixture]
    public class SocketTransportTests : TransportTests<SocketTransport>
    {
        [Test]
        public void CreatingWithSslConfigurationThrowsException()
        {
            var config = new ConnectionConfiguration(sslConfiguration: new SslConfiguration());
            try {
                using (new SocketTransport(TestConfig.ServerEndpoint(), config, TestConfig.Log)) { }
                Assert.Fail("Should have thrown ArgumentOutOfRangeException");
            } catch (ArgumentOutOfRangeException) {
                // expected
            }
        }

        protected override SocketTransport CreateTransport(Endpoint endpoint, IConnectionConfiguration configuration, ILog log)
        {
            return new SocketTransport(endpoint, configuration, log);
        }
    }
}