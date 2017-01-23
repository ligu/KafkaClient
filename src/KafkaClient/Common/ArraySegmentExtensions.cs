using System;
using System.Linq;

namespace KafkaClient.Common
{
    public static class ArraySegmentExtensions
    {
        public static bool HasEqualElementsInOrder(this ArraySegment<byte> self, ArraySegment<byte> other)
        {
            if (self.Count != other.Count) return false;
            if (self.Count == 0) return true;

            return self.Zip(other, (s, o) => Equals(s, o)).All(_ => _);
        }

        public static ArraySegment<T> Skip<T>(this ArraySegment<T> self, int offset)
        {
            return new ArraySegment<T>(self.Array, self.Offset + offset, self.Count - offset);
        }
        
        public static short ToInt16(this ArraySegment<byte> value)
        {
            return BitConverter.ToInt16(value.Array, value.Offset).ToBigEndian();
        }

        public static int ToInt32(this ArraySegment<byte> value)
        {
            return BitConverter.ToInt32(value.Array, value.Offset).ToBigEndian();
        }

        public static long ToInt64(this ArraySegment<byte> value)
        {
            return BitConverter.ToInt64(value.Array, value.Offset).ToBigEndian();
        }

        public static ushort ToUInt16(this ArraySegment<byte> value)
        {
            return BitConverter.ToUInt16(value.Array, value.Offset).ToBigEndian();
        }

        public static uint ToUInt32(this ArraySegment<byte> value)
        {
            return BitConverter.ToUInt32(value.Array, value.Offset).ToBigEndian();
        }

        public static ulong ToUInt64(this ArraySegment<byte> value)
        {
            return BitConverter.ToUInt64(value.Array, value.Offset).ToBigEndian();
        }
    }
}