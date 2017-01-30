using System;

namespace KafkaClient.Protocol
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
        uint ReadCrc(int count);

        ArraySegment<byte> ReadSegment(int count);
        int Position { get; set; }

        bool HasBytes(int count);
    }
}