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
#if true
            using (var writer = new KafkaWriter()) {
                using (var gzip = new GZipStream(new MemoryStream(source.Array, source.Offset, source.Count), CompressionMode.Decompress)) {
                    gzip.CopyTo(writer.Stream);
                    gzip.Flush();
                }
                return writer.ToSegment();
            }
#else
            using (var unzipped = new MemoryStream(CopyBufferSize)) {
                using (var gzip = new GZipStream(new MemoryStream(source.Array, source.Offset, source.Count), CompressionMode.Decompress)) {
                    unzipped.Write(0.ToBytes(), 0, KafkaEncoder.IntegerByteSize); // placeholder for length

                    ArraySegment<byte> bytes;
                    int bytesWritten;
                    do {
                        var capacity = unzipped.Capacity;
                        var offset = (int)unzipped.Position;
                        if (capacity < offset + CopyBufferSize / 2) {
                            capacity += CopyBufferSize;
                            unzipped.SetLength(capacity); // setting capacity rather than length wipes the underlying buffer
                        }
                        unzipped.TryGetBuffer(out bytes);
                        bytesWritten = gzip.Read(bytes.Array, offset, capacity - offset);
                        unzipped.Seek(bytesWritten, SeekOrigin.Current);
                    } while (bytesWritten > 0);

                    if (unzipped.Position > KafkaEncoder.IntegerByteSize) {
                        var totalBytesWritten = (int)unzipped.Position;
                        unzipped.Seek(0, SeekOrigin.Begin);
                        unzipped.Write((totalBytesWritten - KafkaEncoder.IntegerByteSize).ToBytes(), 0, 4); // actual length
                        return new ArraySegment<byte>(bytes.Array, 0, totalBytesWritten);
                    }

                    unzipped.TryGetBuffer(out bytes);
                    return bytes;
                }
            }
#endif
        }
    }
}