using System;
using System.IO;

namespace KafkaClient.Common
{
    public interface IKafkaReader : IDisposable
    {
        long Length { get; }
        long Position { get; set; }
        bool HasData { get; }
        Stream BaseStream { get; }
        bool Available(int dataSize);

        bool ReadBoolean();
        byte ReadByte();
        char ReadChar();
        short ReadInt16();
        int ReadInt32();
        long ReadInt64();
        string ReadString();
        byte[] ReadBytes();
        byte[] ReadToEnd();
        byte[] CrcHash();
        uint Crc();
        byte[] RawRead(int size);
    }
}