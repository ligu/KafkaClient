using System;
using System.Net;
using KafkaClient.Common;
using KafkaClient.Connection;
using KafkaClient.Tests.Helpers;
using NUnit.Framework;

namespace KafkaClient.Tests.Unit
{
    [Category("Unit")]
    [TestFixture]
    public class KafkaEndpointTests
    {
        private readonly IKafkaLog _log = new TraceLog(LogLevel.Warn);

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void EnsureEndpointCanBeResulved()
        {
            var expected = IPAddress.Parse("127.0.0.1");
            var endpoint = new KafkaConnectionFactory().Resolve(new Uri("http://localhost:8888"), _log);
            Assert.That(endpoint.Endpoint.Address, Is.EqualTo(expected));
            Assert.That(endpoint.Endpoint.Port, Is.EqualTo(8888));
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void EnsureTwoEndpointNotOfTheSameReferenceButSameIPAreEqual()
        {
            var endpoint1 = new KafkaConnectionFactory().Resolve(new Uri("http://localhost:8888"), _log);
            var endpoint2 = new KafkaConnectionFactory().Resolve(new Uri("http://localhost:8888"), _log);

            Assert.That(ReferenceEquals(endpoint1, endpoint2), Is.False, "Should not be the same reference.");
            Assert.That(endpoint1, Is.EqualTo(endpoint2));
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void EnsureTwoEndointWithSameIPButDifferentPortsAreNotEqual()
        {
            var endpoint1 = new KafkaConnectionFactory().Resolve(new Uri("http://localhost:8888"), _log);
            var endpoint2 = new KafkaConnectionFactory().Resolve(new Uri("http://localhost:1"), _log);

            Assert.That(endpoint1, Is.Not.EqualTo(endpoint2));
        }
    }
}