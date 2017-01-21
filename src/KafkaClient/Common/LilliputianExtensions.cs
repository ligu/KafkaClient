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

        public static long ToBigEndian(this long value)
        {
            if (!BitConverter.IsLittleEndian) return value;

            var first = (uint)(value >> 32);
            first = ((first << 24) & 0xFF000000) 
                  | ((first <<  8) & 0x00FF0000) 
                  | ((first >>  8) & 0x0000FF00) 
                  | ((first >> 24) & 0x000000FF);
            var second = (uint)value;
            second = ((second << 24) & 0xFF000000) 
                   | ((second <<  8) & 0x00FF0000) 
                   | ((second >>  8) & 0x0000FF00) 
                   | ((second >> 24) & 0x000000FF);

            return ((long) second << 32) | first;
        }

        public static ulong ToBigEndian(this ulong value)
        {
            return BitConverter.IsLittleEndian
                ? (ulong)ToBigEndian((long)value)
                : value;
        }

        public static int ToBigEndian(this int value)
        {
            return BitConverter.IsLittleEndian
                ? (int)ToBigEndian((uint)value)
                : value;
        }

        public static uint ToBigEndian(this uint value)
        {
            return BitConverter.IsLittleEndian
                ? ((value << 24) & 0xFF000000) 
                | ((value <<  8) & 0x00FF0000) 
                | ((value >>  8) & 0x0000FF00) 
                | ((value >> 24) & 0x000000FF)
                : value;
        }

        public static short ToBigEndian(this short value)
        {
            return BitConverter.IsLittleEndian
                ? (short)(((value & 0xFF) << 8) | ((value >> 8) & 0xFF))
                : value;
        }

        public static ushort ToBigEndian(this ushort value)
        {
            return BitConverter.IsLittleEndian
                ? (ushort)ToBigEndian((short)value)
                : value;
        }
    }
}