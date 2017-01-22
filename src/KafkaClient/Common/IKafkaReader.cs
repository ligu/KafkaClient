using System;
using System.IO;

namespace KafkaClient.Common
{
    public interface IKafkaReader : IDisposable
    {
        long Length { get; }
        long Position { get; set; }
        bool HasData { get; }
        Stream Stream { get; }
        bool Available(int dataSize);

        bool ReadBoolean();
        byte ReadByte();
        char ReadChar();
        short ReadInt16();
        int ReadInt32();
        uint ReadUInt32();
        long ReadInt64();
        string ReadString();

        ArraySegment<byte> ReadSegment(int? position = null, int? length = null);
        ArraySegment<byte> ReadBytes();
        uint CrcHash(int? size = null);
    }
}