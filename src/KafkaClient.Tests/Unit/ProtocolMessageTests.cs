using System;
using System.Linq;
using System.Text;
using KafkaClient.Common;
using KafkaClient.Protocol;
using NUnit.Framework;

namespace KafkaClient.Tests.Unit
{
    [TestFixture]
    public class ProtocolMessageTests
    {
        [Test]
        public void DecodeMessageShouldThrowWhenCrcFails()
        {
            var testMessage = new Message(value: "kafka test message.", key: "test");

            using (var writer = new KafkaWriter()) {
                writer.Write(testMessage);
                var encoded = writer.ToSegment(false);
                encoded.Array[encoded.Offset] += 1;
                using (var reader = new BigEndianBinaryReader(encoded)) {
                    Assert.Throws<CrcValidationException>(() => reader.ReadMessage(encoded.Count, 0).First());
                }
            }
        }

        [Test]
        [TestCase("test key", "test message")]
        [TestCase(null, "test message")]
        [TestCase("test key", null)]
        [TestCase(null, null)]
        public void EnsureMessageEncodeAndDecodeAreCompatible(string key, string value)
        {
            var testMessage = new Message(key: key, value: value);

            using (var writer = new KafkaWriter()) {
                writer.Write(testMessage);
                var encoded = writer.ToSegment(false);
                using (var reader = new BigEndianBinaryReader(encoded)) {
                    var result = reader.ReadMessage(encoded.Count, 0).First();

                    Assert.That(testMessage.Key, Is.EqualTo(result.Key));
                    Assert.That(testMessage.Value, Is.EqualTo(result.Value));
                }
            }
        }

        [Test]
        public void EncodeMessageSetEncodesMultipleMessages()
        {
            //expected generated from python library
            var expected = new byte[]
                {
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 16, 45, 70, 24, 62, 0, 0, 0, 0, 0, 1, 49, 0, 0, 0, 1, 48, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 16, 90, 65, 40, 168, 0, 0, 0, 0, 0, 1, 49, 0, 0, 0, 1, 49, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 16, 195, 72, 121, 18, 0, 0, 0, 0, 0, 1, 49, 0, 0, 0, 1, 50
                };

            var messages = new[]
                {
                    new Message("0", "1"),
                    new Message("1", "1"),
                    new Message("2", "1")
                };

            using (var writer = new KafkaWriter()) {
                writer.Write(messages);
                var result = writer.ToSegment(false);
                Assert.That(expected, Is.EqualTo(result));
            }
        }

        [Test]
        public void DecodeMessageSetShouldHandleResponseWithMaxBufferSizeHit()
        {
            using (var reader = new BigEndianBinaryReader(MessageHelper.FetchResponseMaxBytesOverflow)) {
                //This message set has a truncated message bytes at the end of it
                var result = reader.ReadMessages(0);

                var message = result.First().Value.ToUtf8String();

                Assert.That(message, Is.EqualTo("test"));
                Assert.That(result.Count, Is.EqualTo(529));
            }
        }

        [Test]
        public void WhenMessageIsTruncatedThenBufferUnderRunExceptionIsThrown()
        {
            // arrange
            var message = new byte[] { };
            var messageSize = message.Length + 1;
            using (var writer = new KafkaWriter()) {
                writer.Write(0L)
                       .Write(messageSize)
                       .Write(new ArraySegment<byte>(message));
                var segment = writer.ToSegment();
                using (var reader = new BigEndianBinaryReader(segment.Array, segment.Offset, segment.Count)) {
                    // act/assert
                    Assert.Throws<BufferUnderRunException>(() => reader.ReadMessages(0));
                }
            }
        }

        [Test]
        public void WhenMessageIsExactlyTheSizeOfBufferThenMessageIsDecoded()
        {
            // arrange
            var expectedPayloadBytes = new ArraySegment<byte>(new byte[] { 1, 2, 3, 4 });
            using (var writer = new KafkaWriter()) {
                writer.Write(0L);
                using (writer.MarkForLength()) {
                    writer.Write(new Message(expectedPayloadBytes, new ArraySegment<byte>(new byte[] { 0 }), 0, version: 0));
                }
                var segment = writer.ToSegment();

                // act/assert
                using (var reader = new BigEndianBinaryReader(segment.Array, segment.Offset, segment.Count)) {
                    var messages = reader.ReadMessages(0);
                    var actualPayload = messages.First().Value;

                    // assert
                    var expectedPayload = new byte[] { 1, 2, 3, 4 };
                    CollectionAssert.AreEqual(expectedPayload, actualPayload);
                }
            }
        }
    }
}