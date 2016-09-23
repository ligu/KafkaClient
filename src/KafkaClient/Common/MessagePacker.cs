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

        public MessagePacker Pack(int ints)
        {
            _stream.Write(ints);
            return this;
        }

        public MessagePacker Pack(short ints)
        {
            _stream.Write(ints);
            return this;
        }

        public MessagePacker Pack(long ints)
        {
            _stream.Write(ints);
            return this;
        }

        public MessagePacker Pack(byte[] buffer, StringPrefixEncoding encoding = StringPrefixEncoding.Int32)
        {
            _stream.Write(buffer, encoding);
            return this;
        }

        public MessagePacker Pack(string data, StringPrefixEncoding encoding = StringPrefixEncoding.Int32)
        {
            _stream.Write(data, encoding);
            return this;
        }

        public MessagePacker Pack(IEnumerable<string> data, StringPrefixEncoding encoding = StringPrefixEncoding.Int32)
        {
            foreach (var item in data)
            {
                _stream.Write(item, encoding);
            }

            return this;
        }

        public byte[] Payload()
        {
            var buffer = new byte[_stream.BaseStream.Length];
            _stream.BaseStream.Position = 0;
            Pack((int)(_stream.BaseStream.Length - IntegerByteSize));
            _stream.BaseStream.Position = 0;
            _stream.BaseStream.Read(buffer, 0, (int)_stream.BaseStream.Length);
            return buffer;
        }

        public byte[] PayloadNoLength()
        {
            var payloadLength = _stream.BaseStream.Length - IntegerByteSize;
            var buffer = new byte[payloadLength];
            _stream.BaseStream.Position = IntegerByteSize;
            _stream.BaseStream.Read(buffer, 0, (int)payloadLength);
            return buffer;
        }

        public byte[] CrcPayload()
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