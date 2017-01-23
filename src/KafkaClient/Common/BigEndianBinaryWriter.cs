using System;
using System.IO;
using System.Text;

namespace KafkaClient.Common
{
    /// <summary>
    /// A BinaryWriter that stores values in BigEndian format.
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
    /// The code was modified to implement Kafka specific byte handling.
    /// Specifically where STRINGS are 16bit prefixed and BYTE[] are 32bit prefixed
    /// </remarks>
    public class BigEndianBinaryWriter : BinaryWriter
    {
        public BigEndianBinaryWriter(Stream stream)
            : base(stream, Encoding.UTF8)
        {
            if (stream == null) throw new ArgumentNullException(nameof(stream));
        }

        public BigEndianBinaryWriter(Stream stream, bool leaveOpen)
            : base(stream, Encoding.UTF8, leaveOpen)
        {
            if (stream == null) throw new ArgumentNullException(nameof(stream));
        }

        public override void Write(short value)
        {
            base.Write(value.ToBytes());
        }

        public override void Write(int value)
        {
            base.Write(value.ToBytes());
        }

        public override void Write(long value)
        {
            base.Write(value.ToBytes());
        }

        public override void Write(ushort value)
        {
            base.Write(value.ToBytes());
        }

        public override void Write(uint value)
        {
            base.Write(value.ToBytes());
        }

        public override void Write(ulong value)
        {
            base.Write(value.ToBytes());
        }

        public void Write(ArraySegment<byte> value, bool includePrefix)
        {
            if (value.Count == 0) {
                if (includePrefix) {
                    Write(-1);
                }
                return;
            }

            if (includePrefix) {
                Write(value.Count);
            }
            base.Write(value.Array, value.Offset, value.Count);
        }

        public override void Write(string value)
        {
            if (value == null) {
                Write((short)-1);
                return;
            }

            var bytes = Encoding.UTF8.GetBytes(value); 
            Write((short)bytes.Length);
            base.Write(bytes);
        }
    }
}