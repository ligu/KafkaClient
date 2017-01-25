using System;
using System.Net;
using System.Threading.Tasks;
using KafkaClient.Connections;
using NUnit.Framework;

namespace KafkaClient.Tests.Unit
{
    [TestFixture]
    public class KafkaEndpointTests
    {
        [Test]
        public async Task EnsureEndpointCanBeResulved()
        {
            var expected = IPAddress.Parse("127.0.0.1");
            var endpoint = await new ConnectionFactory().ResolveAsync(new Uri("http://localhost:8888"), TestConfig.Log);
            Assert.That(endpoint.Value.Address, Is.EqualTo(expected));
            Assert.That(endpoint.Value.Port, Is.EqualTo(8888));
        }

        [Test]
        public async Task EnsureTwoEndpointNotOfTheSameReferenceButSameIPAreEqual()
        {
            var endpoint1 = await new ConnectionFactory().ResolveAsync(new Uri("http://localhost:8888"), TestConfig.Log);
            var endpoint2 = await new ConnectionFactory().ResolveAsync(new Uri("http://localhost:8888"), TestConfig.Log);

            Assert.That(ReferenceEquals(endpoint1, endpoint2), Is.False, "Should not be the same reference.");
            Assert.That(endpoint1, Is.EqualTo(endpoint2));
        }

        [Test]
        public async Task EnsureTwoEndointWithSameIPButDifferentPortsAreNotEqual()
        {
            var endpoint1 = await new ConnectionFactory().ResolveAsync(new Uri("http://localhost:8888"), TestConfig.Log);
            var endpoint2 = await new ConnectionFactory().ResolveAsync(new Uri("http://localhost:1"), TestConfig.Log);

            Assert.That(endpoint1, Is.Not.EqualTo(endpoint2));
        }
    }
}