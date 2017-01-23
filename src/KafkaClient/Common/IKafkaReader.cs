using System;

namespace KafkaClient.Common
{
    public interface IKafkaReader : IDisposable
    {
        bool ReadBoolean();
        byte ReadByte();
        short ReadInt16();
        int ReadInt32();
        uint ReadUInt32();
        long ReadInt64();
        string ReadString();
        ArraySegment<byte> ReadBytes();

        uint ReadCrc(int? size = null);

        long Position { get; set; }
        bool Available(int dataSize);

        ArraySegment<byte> ReadSegment(int? position = null, int? length = null);
    }
}