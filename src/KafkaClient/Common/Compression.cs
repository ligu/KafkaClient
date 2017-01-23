using System;
using System.IO;
using System.IO.Compression;

namespace KafkaClient.Common
{
    /// <summary>
    /// Extension methods which allow compression of byte arrays
    /// </summary>
    public static class Compression
    {
        public static CompressionLevel ZipLevel { get; set; } = CompressionLevel.Optimal;

        public static void Zip(ArraySegment<byte> bytes, Stream outStream)
        {
            using (var gzip = new GZipStream(outStream, ZipLevel, true)) {
                gzip.Write(bytes.Array, bytes.Offset, bytes.Count);
                gzip.Flush();
            }
        }

        public static ArraySegment<byte> Unzip(this Stream source)
        {
            using (var writer = new KafkaWriter()) {
                using (var gzip = new GZipStream(source, CompressionMode.Decompress, true)) {
                    gzip.CopyTo(writer.Stream);
                    gzip.Flush();
                }
                return writer.ToSegment();
            }
        }
    }
}