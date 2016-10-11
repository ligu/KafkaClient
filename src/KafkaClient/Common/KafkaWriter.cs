using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace KafkaClient.Common
{
    public class KafkaWriter : IKafkaWriter
    {
        private const int IntegerByteSize = 4;
        private readonly BigEndianBinaryWriter _stream;

        public KafkaWriter()
        {
            _stream = new BigEndianBinaryWriter(new MemoryStream());
            Write(IntegerByteSize); //pre-allocate space for buffer length
        }

        public IKafkaWriter Write(byte value)
        {
            _stream.Write(value);
            return this;
        }

        public IKafkaWriter Write(int value)
        {
            _stream.Write(value);
            return this;
        }

        public IKafkaWriter Write(short value)
        {
            _stream.Write(value);
            return this;
        }

        public IKafkaWriter Write(long value)
        {
            _stream.Write(value);
            return this;
        }

        public IKafkaWriter Write(byte[] values, bool includePrefix = true)
        {
            _stream.Write(values, includePrefix);
            return this;
        }

        public IKafkaWriter Write(string value)
        {
            _stream.Write(value);
            return this;
        }

        public IKafkaWriter Write(IEnumerable<string> values, bool includePrefix = false)
        {
            if (includePrefix) {
                var valuesList = values.ToList();
                _stream.Write(valuesList.Count);
                Write(valuesList);
                return this;
            }

            foreach (var item in values) {
                _stream.Write(item);
            }
            return this;
        }

        public byte[] ToBytes()
        {
            var buffer = new byte[_stream.BaseStream.Length];
            _stream.BaseStream.Position = 0;
            Write((int)(_stream.BaseStream.Length - IntegerByteSize));
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