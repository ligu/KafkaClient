using System;
using System.Collections.Generic;
using System.IO;

namespace KafkaClient.Common
{
    public class MessagePacker : IDisposable
    {
        private const int IntegerByteSize = 4;
        private readonly BigEndianBinaryWriter _stream;

        public MessagePacker()
        {
            _stream = new BigEndianBinaryWriter(new MemoryStream());
            Pack(IntegerByteSize); //pre-allocate space for buffer length
        }

        public MessagePacker Pack(byte value)
        {
            _stream.Write(value);
            return this;
        }

        public MessagePacker Pack(int value)
        {
            _stream.Write(value);
            return this;
        }

        public MessagePacker Pack(short value)
        {
            _stream.Write(value);
            return this;
        }

        public MessagePacker Pack(long value)
        {
            _stream.Write(value);
            return this;
        }

        public MessagePacker Pack(byte[] values, bool includePrefix = true)
        {
            _stream.Write(values, includePrefix);
            return this;
        }

        public MessagePacker Pack(string value)
        {
            _stream.Write(value);
            return this;
        }

        public MessagePacker Pack(IEnumerable<string> values)
        {
            foreach (var item in values) {
                _stream.Write(item);
            }

            return this;
        }

        public byte[] ToBytes()
        {
            var buffer = new byte[_stream.BaseStream.Length];
            _stream.BaseStream.Position = 0;
            Pack((int)(_stream.BaseStream.Length - IntegerByteSize));
            _stream.BaseStream.Position = 0;
            _stream.BaseStream.Read(buffer, 0, (int)_stream.BaseStream.Length);
            return buffer;
        }

        public byte[] ToBytesNoLength()
        {
            var payloadLength = _stream.BaseStream.Length - IntegerByteSize;
            var buffer = new byte[payloadLength];
            _stream.BaseStream.Position = IntegerByteSize;
            _stream.BaseStream.Read(buffer, 0, (int)payloadLength);
            return buffer;
        }

        public byte[] ToBytesCrc()
        {
            var buffer = new byte[_stream.BaseStream.Length];

            //copy the payload over
            _stream.BaseStream.Position = 0;
            _stream.BaseStream.Read(buffer, 0, (int)_stream.BaseStream.Length);

            //calculate the crc
            var crc = Crc32Provider.ComputeHash(buffer, IntegerByteSize, buffer.Length);
            buffer[0] = crc[0];
            buffer[1] = crc[1];
            buffer[2] = crc[2];
            buffer[3] = crc[3];

            return buffer;
        }

        public void Dispose()
        {
            using (_stream) { }
        }
    }
}