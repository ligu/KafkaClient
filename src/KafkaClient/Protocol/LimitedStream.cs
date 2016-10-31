using System;
using System.IO;

namespace KafkaClient.Protocol
{
    public class LimitedStream : Stream
    {
        private readonly Stream _stream;
        private readonly long _finalPosition;

        public LimitedStream(Stream stream, int maxRead)
        {
            _stream = stream;
            _finalPosition = _stream.Position + maxRead;
        }

        public override void Flush()
        {
            _stream.Flush();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotImplementedException();
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var toRead = Math.Min(count, (int)(_finalPosition - _stream.Position));
            return _stream.Read(buffer, offset, toRead);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotImplementedException();
        }

        public override bool CanRead => _stream.CanRead;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => _stream.Length;

        public override long Position {
            get { throw new NotImplementedException(); }
            set { throw new NotImplementedException(); }
        }
    }
}