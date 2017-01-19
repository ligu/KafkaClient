using System;
using System.Linq;
using System.Text;

namespace KafkaClient.Common
{
    /// <summary>
    /// Provides Big Endian conversion extensions to required types for the Kafka protocol.
    /// </summary>
    public static class LilliputianExtensions
    {
        public static byte[] ToIntSizedBytes(this string value)
        {
            if (string.IsNullOrEmpty(value)) return (-1).ToBytes();

            return value.Length.ToBytes()
                         .Concat(value.ToBytes())
                         .ToArray();
        }

        public static string ToUtf8String(this byte[] value)
        {
            if (value == null) return string.Empty;

            return Encoding.UTF8.GetString(value);
        }

        public static string ToUtf8String(this ArraySegment<byte> value)
        {
            if (value.Count == 0) return string.Empty;

            return Encoding.UTF8.GetString(value.Array, value.Offset, value.Count);
        }

        public static byte[] ToBytes(this string value)
        {
            if (string.IsNullOrEmpty(value)) return (-1).ToBytes();

            //UTF8 is array of bytes, no endianness
            return Encoding.UTF8.GetBytes(value);
        }

        public static byte[] ToBytes(this int value)
        {
            return BitConverter.GetBytes(value).ToBigEndian();
        }

        public static int ToInt32(this byte[] value)
        {
            const int oneByte = 8;
            const int twoBytes = 16;
            const int threeBytes = 24;
            return BitConverter.IsLittleEndian
                ? (value[0] << threeBytes) | (value[1] << twoBytes) | (value[2] << oneByte) | value[3]
                : (value[3] << threeBytes) | (value[2] << twoBytes) | (value[1] << oneByte) | value[0];
        }

        public static byte[] ToBigEndian(this byte[] bytes)
        {
            if (BitConverter.IsLittleEndian) {
                Array.Reverse(bytes);
            }
            return bytes;
        }
    }
}