using System;
using System.IO;
using System.Net;
using System.Runtime.Serialization.Formatters.Binary;
using KafkaClient.Connections;
using KafkaClient.Protocol;
using NUnit.Framework;

namespace KafkaClient.Tests.Unit
{
    [Category("Unit")]
    [TestFixture]
    public class BinaryFormatterSerializationTests
    {
        [Test]
        public void ShouldSerializeCachedMetadataException()
        {
            var expected = new CachedMetadataException("blblb") { TopicName = "foobar"};
            var actual = SerializeDeserialize(expected);

            Assert.AreEqual(expected.TopicName, actual.TopicName);
        }

        [Test]
        public void ShouldSerializeBufferUnderRunException()
        {
            var expected = new BufferUnderRunException(44, 44, 43);
            var actual = SerializeDeserialize(expected);

            Assert.AreEqual(expected.MessageHeaderSize, actual.MessageHeaderSize);
            Assert.AreEqual(expected.RequiredBufferSize, actual.RequiredBufferSize);
        }

        [Test]
        public void ShouldSerializeOffsetOutOfRangeException()
        {
            var expected = new FetchOutOfRangeException(new Fetch ("aa", 3, 2, 1), ApiKeyRequestType.Fetch, ErrorResponseCode.OffsetOutOfRange);
            var actual = SerializeDeserialize(expected);

            Assert.AreEqual(expected.Fetch.MaxBytes, actual.Fetch.MaxBytes);
            Assert.AreEqual(expected.Fetch.Offset, actual.Fetch.Offset);
            Assert.AreEqual(expected.Fetch.PartitionId, actual.Fetch.PartitionId);
            Assert.AreEqual(expected.Fetch.TopicName, actual.Fetch.TopicName);
        }

        [Test]
        public void ShouldSerializeOffsetOutOfRangeExceptionNull()
        {
            var expected = new FetchOutOfRangeException("a");
            var actual = SerializeDeserialize(expected);

            Assert.AreEqual(expected.Fetch, actual.Fetch);
        }

        [Test]
        public void ShouldSerializeOffsetKafkaEndpointInnerObjectAreNull()
        {
            var expected = new RequestException("a");
            var actual = SerializeDeserialize(expected);

            Assert.AreEqual(expected.Endpoint, actual.Endpoint);
        }

        [Test]
        public void ShouldSerializeOffsetKafkaEndpoint()
        {
            var expected = new RequestException("a") {
                Endpoint = new Endpoint(new Uri("http://S1.com"), 
                new IPEndPoint(IPAddress.Parse("127.0.0.1"), 8888))
            };
            var actual = SerializeDeserialize(expected);

            Assert.AreEqual(expected.Endpoint.ServerUri, actual.Endpoint.ServerUri);
            Assert.AreEqual(expected.Endpoint.IP, actual.Endpoint.IP);
        }

        [Test]
        public void ShouldSerializeOffsetKafkaEndpointNull()
        {
            var expected = new RequestException("a", null);
            var actual = SerializeDeserialize(expected);

            Assert.AreEqual(expected.Endpoint, actual.Endpoint);
        }

        [Test]
        public void ShouldSerializeKafkaApplicationException()
        {
            var expected = new RequestException(ApiKeyRequestType.Fetch, ErrorResponseCode.OffsetOutOfRange, "3");
            var actual = SerializeDeserialize(expected);

            Assert.AreEqual(expected.ErrorCode, actual.ErrorCode);
        }

        private static T SerializeDeserialize<T> (T expected)
        {
            var formatter = new BinaryFormatter();
            MemoryStream memoryStream = new MemoryStream();
            formatter.Serialize(memoryStream, expected);
            memoryStream.Seek(0, 0);

            var actual = (T)formatter.Deserialize(memoryStream);
            return actual;
        }
    }
}