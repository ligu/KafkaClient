using System;
using System.IO;
using System.IO.Compression;
using KafkaClient.Protocol;

namespace KafkaClient.Common
{
    /// <summary>
    /// Extension methods which allow compression of byte arrays
    /// </summary>
    public static class Compression
    {
        public static CompressionLevel ZipLevel { get; set; } = CompressionLevel.Fastest;

        public static void Zip(ArraySegment<byte> bytes, IKafkaWriter writer)
        {
            using (var gzip = new GZipStream(writer.Stream, ZipLevel, true)) {
                gzip.Write(bytes.Array, bytes.Offset, bytes.Count);
                gzip.Flush();
            }
        }

        /// <summary>
        /// Pick a value that's a multiple of 4096, where its double is less than the large object heap threshold (85K).
        /// </summary>
        public static int CopyBufferSize { get; set; } = 81920;

        public static ArraySegment<byte> Unzip(this ArraySegment<byte> source)
        {
#if false
            using (var writer = new KafkaWriter()) {
                using (var gzip = new GZipStream(new MemoryStream(source.Array, source.Offset, source.Count), CompressionMode.Decompress)) {
                    gzip.CopyTo(writer.Stream);
                    gzip.Flush();
                }
                return writer.ToSegment();
            }
#elif false

            var bytes = new byte[CopyBufferSize];
            var offset = KafkaEncoder.IntegerByteSize; // placeholder for length
            using (var gzip = new GZipStream(new MemoryStream(source.Array, source.Offset, source.Count), CompressionMode.Decompress)) {
                int bytesWritten;
                do {
                    if (bytes.Length < offset + CopyBufferSize / 2) {
                        var expandedBytes = new byte[bytes.Length + CopyBufferSize];
                        Buffer.BlockCopy(bytes, KafkaEncoder.IntegerByteSize, expandedBytes, KafkaEncoder.IntegerByteSize, offset - KafkaEncoder.IntegerByteSize);
                        bytes = expandedBytes;
                    }
                    bytesWritten = gzip.Read(bytes, offset, bytes.Length - offset);
                    offset += bytesWritten;
                } while (bytesWritten > 0);

                var lengthBytes = (offset - KafkaEncoder.IntegerByteSize).ToBytes(); 
                Buffer.BlockCopy(lengthBytes, 0, bytes, 0, KafkaEncoder.IntegerByteSize);  // actual length
                return new ArraySegment<byte>(bytes, 0, offset);
            }
#else
            var uncompressedLength = 0;
            using (var gzip = new GZipStream(new MemoryStream(source.Array, source.Offset, source.Count), CompressionMode.Decompress)) {
                var bytes = new byte[CopyBufferSize + KafkaEncoder.IntegerByteSize];
                int bytesWritten, offset = KafkaEncoder.IntegerByteSize;
                do {
                    bytesWritten = gzip.Read(bytes, offset, bytes.Length - offset);
                    if (offset + bytesWritten < bytes.Length) {
                        offset += bytesWritten;
                    } else {
                        offset = 0;
                    }
                    uncompressedLength += bytesWritten;
                } while (bytesWritten > 0);

                if (uncompressedLength <= CopyBufferSize) {
                    // it fit in the first buffer, so no need to reallocate
                    offset = 0;
                    foreach (var lengthByte in uncompressedLength.ToBytes()) {
                        bytes[offset++] = lengthByte;
                    }
                    return new ArraySegment<byte>(bytes, 0, KafkaEncoder.IntegerByteSize + uncompressedLength);
                }
            }

            using (var gzip = new GZipStream(new MemoryStream(source.Array, source.Offset, source.Count), CompressionMode.Decompress)) {
                var lengthBytes = uncompressedLength.ToBytes(); 
                var bytes = new byte[uncompressedLength + lengthBytes.Length];
                var offset = 0;
                foreach (var lengthByte in uncompressedLength.ToBytes()) {
                    bytes[offset++] = lengthByte;
                }

                int bytesWritten;
                do {
                    bytesWritten = gzip.Read(bytes, offset, bytes.Length - offset);
                    offset += bytesWritten;
                } while (bytesWritten > 0);
                return new ArraySegment<byte>(bytes);
            }
#endif
        }
    }
}