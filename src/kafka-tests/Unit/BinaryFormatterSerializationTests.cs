using KafkaNet.Model;
using KafkaNet.Protocol;
using NUnit.Framework;
using System;
using System.IO;
using System.Net;
using System.Runtime.Serialization.Formatters.Binary;

namespace kafka_tests.Unit
{
    [TestFixture]
    public class BinaryFormatterSerializationTests
    {
        [Test]
        public void ShouldSerializeInvalidTopicMetadataException()
        {
            var expected = new InvalidTopicMetadataException(ErrorResponseCode.RequestTimedOut, "blblb");
            var actual = SerializeDeserialize(expected);

            Assert.AreEqual(expected.ErrorResponseCode, actual.ErrorResponseCode);
        }

        [Test]
        public void ShouldSerializeBufferUnderRunException()
        {
            var expected = new BufferUnderRunException(44,44);
            var actual = SerializeDeserialize(expected);

            Assert.AreEqual(expected.MessageHeaderSize, actual.MessageHeaderSize);
            Assert.AreEqual(expected.RequiredBufferSize, actual.RequiredBufferSize);
        }

        [Test]
        public void ShouldSerializeOffsetOutOfRangeException()
        {
            var expected = new OffsetOutOfRangeException("a"){FetchRequest = new Fetch(){MaxBytes = 1,Topic = "aa",Offset = 2,PartitionId = 3}};
            var actual = SerializeDeserialize(expected);

            Assert.AreEqual(expected.FetchRequest.MaxBytes, actual.FetchRequest.MaxBytes);
            Assert.AreEqual(expected.FetchRequest.Offset, actual.FetchRequest.Offset);
            Assert.AreEqual(expected.FetchRequest.PartitionId, actual.FetchRequest.PartitionId);
            Assert.AreEqual(expected.FetchRequest.Topic, actual.FetchRequest.Topic);
        }

        [Test]
        public void ShouldSerializeOffsetOutOfRangeExceptionNull()
        {
            var expected = new OffsetOutOfRangeException("a") {FetchRequest = null};
            var actual = SerializeDeserialize(expected);

            Assert.AreEqual(expected.FetchRequest, actual.FetchRequest);
        }

        [Test]
        public void ShouldSerializeOffsetKafkaEndpointInnerObjectAreNull()
        {
            var expected = new BrokerException("a",new KafkaEndpoint());
            var actual = SerializeDeserialize(expected);

            Assert.AreEqual(expected.BrokerEndPoint.ServeUri, actual.BrokerEndPoint.ServeUri);
            Assert.AreEqual(expected.BrokerEndPoint.Endpoint, actual.BrokerEndPoint.Endpoint);
        }

        [Test]
        public void ShouldSerializeOffsetKafkaEndpoint()
        {
            var expected = new BrokerException("a", new KafkaEndpoint() {Endpoint = new IPEndPoint(IPAddress.Parse("127.0.0.1"), 8888),ServeUri = new Uri("http://S1.com")});
            var actual = SerializeDeserialize(expected);

            Assert.AreEqual(expected.BrokerEndPoint.ServeUri, actual.BrokerEndPoint.ServeUri);
            Assert.AreEqual(expected.BrokerEndPoint.Endpoint, actual.BrokerEndPoint.Endpoint);
        }

        [Test]
        public void ShouldSerializeOffsetKafkaEndpointNull()
        {
            var expected = new BrokerException("a", null);
            var actual = SerializeDeserialize(expected);

            Assert.AreEqual(expected.BrokerEndPoint, actual.BrokerEndPoint);
        }

        [Test]
        public void ShouldSerializeKafkaApplicationException()
        {
            var expected = new KafkaApplicationException("3"){ErrorCode = 1};
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