using System;

namespace KafkaClient.Common
{
    /// <summary>
    /// Provides Big Endian conversion extensions to required types for the Kafka protocol.
    /// </summary>
    public static class LilliputianExtensions
    {
        public static byte[] ToBytes(this short value)
        {
            return BitConverter.GetBytes(value.ToBigEndian());
        }

        public static byte[] ToBytes(this int value)
        {
            return BitConverter.GetBytes(value.ToBigEndian());
        }

        public static byte[] ToBytes(this long value)
        {
            return BitConverter.GetBytes(value.ToBigEndian());
        }

        public static byte[] ToBytes(this ushort value)
        {
            return BitConverter.GetBytes(value.ToBigEndian());
        }

        public static byte[] ToBytes(this uint value)
        {
            return BitConverter.GetBytes(value.ToBigEndian());
        }

        public static byte[] ToBytes(this ulong value)
        {
            return BitConverter.GetBytes(value.ToBigEndian());
        }

        public static short ToInt16(this byte[] value)
        {
            return BitConverter.ToInt16(value, 0).ToBigEndian();
        }

        public static int ToInt32(this byte[] value)
        {
            return BitConverter.ToInt32(value, 0).ToBigEndian();
        }

        public static long ToInt64(this byte[] value)
        {
            return BitConverter.ToInt64(value, 0).ToBigEndian();
        }

        public static ushort ToUInt16(this byte[] value)
        {
            return BitConverter.ToUInt16(value, 0).ToBigEndian();
        }

        public static uint ToUInt32(this byte[] value)
        {
            return BitConverter.ToUInt32(value, 0).ToBigEndian();
        }

        public static ulong ToUInt64(this byte[] value)
        {
            return BitConverter.ToUInt64(value, 0).ToBigEndian();
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