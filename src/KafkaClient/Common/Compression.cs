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

        public static void WriteGzip(this IKafkaWriter writer, ArraySegment<byte> bytes)
        {
            using (var gzip = new GZipStream(writer.Stream, ZipLevel, true)) {
                gzip.Write(bytes.Array, bytes.Offset, bytes.Count);
                gzip.Flush();
            }
        }

        public static void WriteSnappy(this IKafkaWriter writer, ArraySegment<byte> bytes)
        {
#if DOTNETSTANDARD
            var buffer = new byte[Snappy.SnappyCodec.GetMaxCompressedLength(bytes.Count)];
            var size = Snappy.SnappyCodec.Compress(bytes.Array, bytes.Offset, bytes.Count, buffer, 0);
            writer.Write(new ArraySegment<byte>(buffer, 0, size), false);
#else
            throw new NotSupportedException("Snappy codec is not supported except on .net core");
#endif
        }

        public static ArraySegment<byte> ReadGzip(this ArraySegment<byte> source)
        {
            using (var writer = new KafkaWriter()) {
                using (var gzip = new GZipStream(new MemoryStream(source.Array, source.Offset, source.Count), CompressionMode.Decompress)) {
                    gzip.CopyTo(writer.Stream);
                    gzip.Flush();
                }
                return writer.ToSegment();
            }
        }

        public static ArraySegment<byte> ReadSnappy(this ArraySegment<byte> source)
        {
#if DOTNETSTANDARD
            var buffer = new byte[Snappy.SnappyCodec.GetUncompressedLength(source.Array, source.Offset, source.Count)];
            var size = Snappy.SnappyCodec.Uncompress(source.Array, source.Offset, source.Count, buffer, 0);
            return new ArraySegment<byte>(buffer, 0, size);
#else
            throw new NotSupportedException("Snappy codec is not supported except on .net core");
#endif
        }
    }
}