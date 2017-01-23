using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using KafkaClient.Protocol;

namespace KafkaClient.Common
{
    public class KafkaWriter : IKafkaWriter
    {
        private readonly BigEndianBinaryWriter _stream;
        private readonly MemoryStream _memStream;
        private bool _leaveOpen = false;

        public KafkaWriter()
        {
            _memStream = new MemoryStream();
            _stream = new BigEndianBinaryWriter(_memStream, true);
            Write(KafkaEncoder.IntegerByteSize); //pre-allocate space for buffer length
        }

        public IKafkaWriter Write(bool value)
        {
            _memStream.WriteByte(value ? (byte)1 : (byte)0);
            return this;
        }

        public IKafkaWriter Write(byte value)
        {
            _memStream.WriteByte(value);
            return this;
        }

        public IKafkaWriter Write(short value)
        {
            _memStream.Write(value.ToBytes(), 0, 2);
            return this;
        }

        public IKafkaWriter Write(int value)
        {
            _memStream.Write(value.ToBytes(), 0, 4);
            return this;
        }

        public IKafkaWriter Write(long value)
        {
            _memStream.Write(value.ToBytes(), 0, 8);
            return this;
        }

        public IKafkaWriter Write(ArraySegment<byte> value, bool includeLength = true)
        {
            if (value.Count == 0) {
                if (includeLength) {
                    Write(-1);
                }
                return this;
            }

            if (includeLength) {
                Write(value.Count);
            }
            _memStream.Write(value.Array, value.Offset, value.Count);
            return this;
        }

        public IKafkaWriter Write(string value)
        {
            if (value == null) {
                Write((short)-1);
                return this;
            }

            var bytes = Encoding.UTF8.GetBytes(value); 
            Write((short)bytes.Length);
            _memStream.Write(bytes, 0, bytes.Length);
            return this;
        }

        public IKafkaWriter Write(IEnumerable<string> values, bool includeLength = false)
        {
            if (includeLength) {
                var valuesList = values.ToList();
                Write(valuesList.Count);
                Write(valuesList); // NOTE: !includeLength passed next time
                return this;
            }

            foreach (var item in values) {
                Write(item);
            }
            return this;
        }

        private byte[] ToBytes(int offset)
        {
            var length = _stream.BaseStream.Length - offset;
            var buffer = new byte[length];
            _stream.BaseStream.Position = offset;
            _stream.BaseStream.Read(buffer, 0, (int)length);
            return buffer;
        }

        public ArraySegment<byte> ToSegment(bool includeLength = true)
        {
            if (includeLength) {
                WriteLength(0);
                return ToSegment(0);
            }
            return ToSegment(KafkaEncoder.IntegerByteSize);
        }

        private ArraySegment<byte> ToSegment(int offset)
        {
            ArraySegment<byte> segment;
            if (_memStream.TryGetBuffer(out segment)) {
                return segment.Skip(offset);
            }
            var buffer = ToBytes(offset);
            return new ArraySegment<byte>(buffer, 0, buffer.Length);
        }

        public Stream ToStream()
        {
            WriteLength(0);
            _stream.BaseStream.Position = 0;
            _leaveOpen = true;
            return _stream.BaseStream;
        }

        private void WriteLength(int offset)
        {
            _stream.BaseStream.Position = offset;
            var length = (int)_stream.BaseStream.Length - (offset + KafkaEncoder.IntegerByteSize); 
            Write(length);
        }

        private void WriteCrc(int offset)
        {
            uint crc;
            ArraySegment<byte> segment;
            var computeFrom = offset + KafkaEncoder.IntegerByteSize;
            if (_memStream.TryGetBuffer(out segment)) {
                crc = Crc32Provider.ComputeHash(segment.Skip(computeFrom));
            } else {
                _stream.BaseStream.Position = computeFrom;
                crc = Crc32Provider.ComputeHash(_stream.BaseStream.ToEnumerable());
            }

            _stream.BaseStream.Position = offset;
            _stream.Write(crc);            
        }

        public IDisposable MarkForLength()
        {
            var markerPosition = (int)_stream.BaseStream.Position;
            _stream.BaseStream.Seek(KafkaEncoder.IntegerByteSize, SeekOrigin.Current); //pre-allocate space for marker
            return new WriteAt(this, WriteLength, markerPosition);
        }

        public IDisposable MarkForCrc()
        {
            var markerPosition = (int)_stream.BaseStream.Position;
            _stream.BaseStream.Seek(KafkaEncoder.IntegerByteSize, SeekOrigin.Current); //pre-allocate space for marker
            return new WriteAt(this, WriteCrc, markerPosition);
        }

        private class WriteAt : IDisposable
        {
            private readonly KafkaWriter _writer;
            private readonly int _position;
            private readonly Action<int> _write;

            public WriteAt(KafkaWriter writer, Action<int> write, int position)
            {
                _writer = writer;
                _position = position;
                _write = write;
            }

            public void Dispose()
            {
                _write(_position);
                _writer._stream.BaseStream.Seek(0, SeekOrigin.End);
            }
        }

        public void Dispose()
        {
            using (_stream) {
                if (!_leaveOpen) {
                    using (_memStream) {
                    }
                }
            }
        }

        public Stream Stream => _stream.BaseStream;
    }
}