﻿using System;
using System.Net;
using System.Threading.Tasks;
using KafkaClient.Connections;
using NUnit.Framework;

namespace KafkaClient.Tests.Unit
{
    [TestFixture]
    public class EndpointTests
    {
        [Test]
        public void ThrowsExceptionIfIpNull()
        {
            Assert.Throws<ArgumentNullException>(
                () => {
                    var x = new Endpoint(null);
                });
        }

        [Test]
        public async Task EnsureEndpointCanBeResolved()
        {
            var expected = IPAddress.Parse("127.0.0.1");
            var endpoint = await Endpoint.ResolveAsync(new Uri("tcp://localhost:8888"), TestConfig.Log);
            Assert.That(endpoint.Ip.Address, Is.EqualTo(expected));
            Assert.That(endpoint.Ip.Port, Is.EqualTo(8888));
        }

        [Test]
        public async Task EnsureTwoEndpointNotOfTheSameReferenceButSameIPAreEqual()
        {
            var endpoint1 = await Endpoint.ResolveAsync(new Uri("tcp://localhost:8888"), TestConfig.Log);
            var endpoint2 = await Endpoint.ResolveAsync(new Uri("tcp://localhost:8888"), TestConfig.Log);

            Assert.That(ReferenceEquals(endpoint1, endpoint2), Is.False, "Should not be the same reference.");
            Assert.That(endpoint1, Is.EqualTo(endpoint2));
        }

        [Test]
        public async Task EnsureTwoEndointWithSameIPButDifferentPortsAreNotEqual()
        {
            var endpoint1 = await Endpoint.ResolveAsync(new Uri("tcp://localhost:8888"), TestConfig.Log);
            var endpoint2 = await Endpoint.ResolveAsync(new Uri("tcp://localhost:1"), TestConfig.Log);

            Assert.That(endpoint1, Is.Not.EqualTo(endpoint2));
        }
    }
}