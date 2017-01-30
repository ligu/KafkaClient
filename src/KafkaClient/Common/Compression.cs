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

        public static void WriteZipped(this IKafkaWriter writer, ArraySegment<byte> bytes)
        {
            using (var gzip = new GZipStream(writer.Stream, ZipLevel, true)) {
                gzip.Write(bytes.Array, bytes.Offset, bytes.Count);
                gzip.Flush();
            }
        }

        public static ArraySegment<byte> Unzip(this ArraySegment<byte> source)
        {
            using (var writer = new KafkaWriter()) {
                using (var gzip = new GZipStream(new MemoryStream(source.Array, source.Offset, source.Count), CompressionMode.Decompress)) {
                    gzip.CopyTo(writer.Stream);
                    gzip.Flush();
                }
                return writer.ToSegment();
            }
        }
    }
}