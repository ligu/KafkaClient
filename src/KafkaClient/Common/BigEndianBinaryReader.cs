using System;
using System.IO;
using System.Text;

namespace KafkaClient.Common
{
    /// <summary>
    /// A BinaryReader that is BigEndian aware binary reader.
    /// </summary>
    /// <remarks>
    /// Booleans, bytes and byte arrays will be written directly.
    /// All other values will be converted to a byte array in BigEndian byte order and written.
    /// Characters and Strings will all be encoded in UTF-8 (which is byte order independent).
    /// </remarks>
    /// <remarks>
    /// BigEndianBinaryWriter code provided by Zoltu
    /// https://github.com/Zoltu/Zoltu.EndianAwareBinaryReaderWriter
    ///
    /// The code was modified to provide Kafka specific logic and helper functions.
    /// Specifically where STRINGS are 16bit prefixed and BYTE[] are 32bit prefixed
    /// </remarks>
    public class BigEndianBinaryReader : BinaryReader, IKafkaReader
    {
        private const int KafkaNullSize = -1;
        private readonly MemoryStream _memStream;

        public BigEndianBinaryReader(Stream stream, bool leaveOpen)
            : base(stream, Encoding.UTF8, leaveOpen)
        {
            _memStream = stream as MemoryStream;
        }

        public BigEndianBinaryReader(byte[] payload, int offset, int count)
            : this(new MemoryStream(payload, offset, count), false)
        {
        }

        public BigEndianBinaryReader(byte[] payload) : this(payload, 0, payload.Length)
        {
        }

        public BigEndianBinaryReader(ArraySegment<byte> bytes) : this(bytes.Array, bytes.Offset, bytes.Count)
        {
        }

        public long Length => BaseStream.Length;
        public long Position { get { return BaseStream.Position; } set { BaseStream.Position = value; } }

        public bool Available(int dataSize)
        {
            return BaseStream.Length - BaseStream.Position >= dataSize;
        }

        public override short ReadInt16()
        {
            return GetNextBytes(2).ToInt16();
        }

        public override int ReadInt32()
        {
            return GetNextBytes(4).ToInt32();
        }

        public override long ReadInt64()
        {
            return GetNextBytes(8).ToInt64();
        }

        public override ushort ReadUInt16()
        {
            return GetNextBytes(2).ToUInt16();
        }

        public override uint ReadUInt32()
        {
            return GetNextBytes(4).ToUInt32();
        }

        public override ulong ReadUInt64()
        {
            return GetNextBytes(8).ToUInt64();
        }

        public override string ReadString()
        {
            var size = ReadInt16();
            if (size == KafkaNullSize) return null;

            var segment = ReadSegment((int)Position, size);
            var result = Encoding.UTF8.GetString(segment.Array, segment.Offset, segment.Count);
            Position += segment.Count;
            return result;
        }

        public ArraySegment<byte> ReadBytes()
        {
            var size = ReadInt32();
            if (size == KafkaNullSize) { return EmptySegment; }
            return ReadSegment(length: size);
        }

        public uint ReadCrc(int? size = null)
        {
            var old = Position;
            try {
                var segment = ReadSegment(length: size);
                return Crc32Provider.ComputeHash(segment);
            } finally {
                Position = old;
            }
        }

        private static readonly ArraySegment<byte> EmptySegment = new ArraySegment<byte>(new byte[0]);

        public ArraySegment<byte> ReadSegment(int? position = null, int? length = null)
        {
            if (position.HasValue && position.Value > BaseStream.Length) return EmptySegment;

            var offset = position.GetValueOrDefault((int) Position);
            var count = length.HasValue
                ? Math.Min(length.Value, (int)Length - offset)
                : (int)Length - offset;

            if (count < 0) throw new EndOfStreamException();

            ArraySegment<byte> segment;
            if (_memStream != null && _memStream.TryGetBuffer(out segment)) {
                if (!position.HasValue) {
                    BaseStream.Seek(count, SeekOrigin.Current);
                }
                return new ArraySegment<byte>(segment.Array, segment.Offset + offset, count);
            }

            var buffer = new byte[count];
            if (position.HasValue) {
                // need to reset position after reading
                var old = Position;
                try {
                    Position = offset;
                    Read(buffer, 0, buffer.Length);
                } finally {
                    Position = old;
                }
            } else {
                Read(buffer, 0, buffer.Length);
            }
            return new ArraySegment<byte>(buffer);
        }

        private T EndianAwareRead<T>(int size, Func<byte[], int, T> converter) where T : struct
        {
            if (size < 0) throw new ArgumentOutOfRangeException(nameof(size), size, "Must be >= 0");
            if (converter == null) throw new ArgumentNullException(nameof(converter));

            var bytes = GetNextBytesNativeEndian(size);
            return converter(bytes, 0);
        }

        private byte[] GetNextBytesNativeEndian(int count)
        {
            if (count < 0) throw new ArgumentOutOfRangeException(nameof(count), count, "Must be >= 0");

            var bytes = GetNextBytes(count);
            if (BitConverter.IsLittleEndian) {
                Array.Reverse(bytes);
            }
            return bytes;
        }

        private byte[] GetNextBytes(int count)
        {
            if (count < 0) throw new ArgumentOutOfRangeException(nameof(count), count, "Must be >= 0");

            var buffer = new byte[count];
            var bytesRead = BaseStream.Read(buffer, 0, count);

            if (bytesRead != count) throw new EndOfStreamException();

            return buffer;
        }
    }
}