using System.IO;
using KafkaClient.Common;
using NUnit.Framework;

namespace KafkaClient.Tests.Unit
{
    /// <summary>
    /// BigEndianBinaryWriter code provided by Zoltu
    /// https://github.com/Zoltu/Zoltu.EndianAwareBinaryReaderWriter
    /// </summary>
    /// <remarks>Modified to work with nunit from xunit.</remarks>
    [TestFixture]
    public class BigEndianBinaryReaderTests
    {
        // validates my assumptions about the default implementation doing the opposite of this implementation
        [Test]
        [TestCase((int)0, new byte[] { 0x00, 0x00, 0x00, 0x00 })]
        [TestCase((int)1, new byte[] { 0x01, 0x00, 0x00, 0x00 })]
        [TestCase((int)(-1), new byte[] { 0xFF, 0xFF, 0xFF, 0xFF })]
        [TestCase(int.MinValue, new byte[] { 0x00, 0x00, 0x00, 0x80 })]
        [TestCase(int.MaxValue, new byte[] { 0xFF, 0xFF, 0xFF, 0x7F })]
        public void NativeBinaryWriterTests(int expectedValue, byte[] givenBytes)
        {
            // arrange
            var binaryReader = new BinaryReader(new MemoryStream(givenBytes));

            // act
            var actualValue = binaryReader.ReadInt32();

            // assert
            Assert.That(actualValue, Is.EqualTo(expectedValue));
        }

        [Test]
        [TestCase((short)0, new byte[] { 0x00, 0x00 })]
        [TestCase((short)1, new byte[] { 0x00, 0x01 })]
        [TestCase((short)256, new byte[] { 0x01, 0x00 })]
        [TestCase((short)16295, new byte[] { 0x3F, 0xA7 })]
        [TestCase((short)(-1), new byte[] { 0xFF, 0xFF })]
        [TestCase(short.MinValue, new byte[] { 0x80, 0x00 })]
        [TestCase(short.MaxValue, new byte[] { 0x7F, 0xFF })]
        public void Int16Tests(short expectedValue, byte[] givenBytes)
        {
            // arrange
            var binaryReader = new BigEndianBinaryReader(givenBytes);

            // act
            var actualValue = binaryReader.ReadInt16();

            // assert
            Assert.That(actualValue, Is.EqualTo(expectedValue));
        }

        [Test]
        [TestCase((int)0, new byte[] { 0x00, 0x00, 0x00, 0x00 })]
        [TestCase((int)1, new byte[] { 0x00, 0x00, 0x00, 0x01 })]
        [TestCase((int)256, new byte[] { 0x00, 0x00, 0x01, 0x00 })]
        [TestCase((int)258, new byte[] { 0x00, 0x00, 0x01, 0x02 })]
        [TestCase((int)67305985, new byte[] { 0x04, 0x03, 0x02, 0x01 })]
        [TestCase((int)(-1), new byte[] { 0xFF, 0xFF, 0xFF, 0xFF })]
        [TestCase(int.MinValue, new byte[] { 0x80, 0x00, 0x00, 0x00 })]
        [TestCase(int.MaxValue, new byte[] { 0x7F, 0xFF, 0xFF, 0xFF })]
        public void Int32Tests(int expectedValue, byte[] givenBytes)
        {
            // arrange
            var binaryReader = new BigEndianBinaryReader(givenBytes);

            // act
            var actualValue = binaryReader.ReadInt32();

            // assert
            Assert.That(actualValue, Is.EqualTo(expectedValue));
        }

        [Test]
        [TestCase((long)0L, new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })]
        [TestCase((long)1L, new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01 })]
        [TestCase((long)258L, new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02 })]
        [TestCase((long)1234567890123L, new byte[] { 0x00, 0x00, 0x01, 0x1F, 0x71, 0xFB, 0x04, 0xCB })]
        [TestCase((long)(-1L), new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF })]
        [TestCase(long.MinValue, new byte[] { 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })]
        [TestCase(long.MaxValue, new byte[] { 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF })]
        public void Int64Tests(long expectedValue, byte[] givenBytes)
        {
            // arrange
            var binaryReader = new BigEndianBinaryReader(givenBytes);

            // act
            var actualValue = binaryReader.ReadInt64();

            // assert
            Assert.That(actualValue, Is.EqualTo(expectedValue));
        }

        [Test]
        [TestCase((uint)0, new byte[] { 0x00, 0x00, 0x00, 0x00 })]
        [TestCase((uint)1, new byte[] { 0x00, 0x00, 0x00, 0x01 })]
        [TestCase((uint)123456789, new byte[] { 0x07, 0x5B, 0xCD, 0x15 })]
        [TestCase((uint)0xffffffff, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF })]
        public void UInt32Tests(uint expectedValue, byte[] givenBytes)
        {
            // arrange
            var binaryReader = new BigEndianBinaryReader(givenBytes);

            // act
            var actualValue = binaryReader.ReadUInt32();

            // assert
            Assert.That(actualValue, Is.EqualTo(expectedValue));
        }

        [Test]
        [TestCase((float)(0), new byte[] { 0x00, 0x00, 0x00, 0x00 })]
        [TestCase((float)(1), new byte[] { 0x3F, 0x80, 0x00, 0x00 })]
        [TestCase((float)(-1), new byte[] { 0xBF, 0x80, 0x00, 0x00 })]
        [TestCase(float.MinValue, new byte[] { 0xFF, 0x7F, 0xFF, 0xFF })]
        [TestCase(float.MaxValue, new byte[] { 0x7F, 0x7F, 0xFF, 0xFF })]
        [TestCase(float.PositiveInfinity, new byte[] { 0x7F, 0x80, 0x00, 0x00 })]
        [TestCase(float.NegativeInfinity, new byte[] { 0xFF, 0x80, 0x00, 0x00 })]
        [TestCase(float.NaN, new byte[] { 0xFF, 0xC0, 0x00, 0x00 })]
        public void SingleTests(float expectedValue, byte[] givenBytes)
        {
            // arrange
            var binaryReader = new BigEndianBinaryReader(givenBytes);

            // act
            var actualValue = binaryReader.ReadSingle();

            // assert
            Assert.That(actualValue, Is.EqualTo(expectedValue));
        }

        [Test]
        [TestCase((double)(0), new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })]
        [TestCase((double)(1), new byte[] { 0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })]
        [TestCase((double)(-1), new byte[] { 0xBF, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })]
        [TestCase(double.MinValue, new byte[] { 0xFF, 0xEF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF })]
        [TestCase(double.MaxValue, new byte[] { 0x7F, 0xEF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF })]
        [TestCase(double.PositiveInfinity, new byte[] { 0x7F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })]
        [TestCase(double.NegativeInfinity, new byte[] { 0xFF, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })]
        [TestCase(double.NaN, new byte[] { 0xFF, 0xF8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })]
        public void DoubleTests(double expectedValue, byte[] givenBytes)
        {
            // arrange
            var binaryReader = new BigEndianBinaryReader(givenBytes);

            // act
            var actualValue = binaryReader.ReadDouble();

            // assert
            Assert.That(expectedValue, Is.EqualTo(actualValue));
        }

        [Test]
        [TestCase("0000", new byte[] { 0x00, 0x04, 0x30, 0x30, 0x30, 0x30 })]
        [TestCase("€€€€", new byte[] { 0x00, 0x0C, 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC })]
        [TestCase("", new byte[] { 0x00, 0x00 })]
        [TestCase(null, new byte[] { 0xFF, 0xFF })]
        public void StringTests(string expectedValue, byte[] givenBytes)
        {
            // arrange
            var binaryReader = new BigEndianBinaryReader(givenBytes);

            // act
            var actualValue = binaryReader.ReadString();

            // assert
            Assert.That(expectedValue, Is.EqualTo(actualValue));
        }

        [Test]
        [TestCase((char)'0', new byte[] { 0x30 })]
        [TestCase((char)'€', new byte[] { 0xE2, 0x82, 0xAC })]
        public void CharTests(char expectedValue, byte[] givenBytes)
        {
            // arrange
            var binaryReader = new BigEndianBinaryReader(givenBytes);

            // act
            var actualValue = binaryReader.ReadChar();

            // assert
            Assert.That(expectedValue, Is.EqualTo(actualValue));
        }

        [Test]
        [TestCase(new char[] { '0', '0', '0', '0' }, new byte[] { 0x30, 0x30, 0x30, 0x30 })]
        [TestCase(new char[] { '€', '€', '€', '€' }, new byte[] { 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC })]
        public void CharArrayTests(char[] expectedValue, byte[] givenBytes)
        {
            // arrange
            var binaryReader = new BigEndianBinaryReader(givenBytes);

            // act
            var actualValue = binaryReader.ReadChars(givenBytes.Length);

            // assert
            Assert.That(expectedValue, Is.EqualTo(actualValue));
        }
    }
}