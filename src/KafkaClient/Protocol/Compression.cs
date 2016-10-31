using System;
using System.IO;
using System.IO.Compression;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// Extension methods which allow compression of byte arrays
    /// </summary>
    public static class Compression
    {
        public static byte[] Zip(byte[] bytes)
        {
            using (var destination = new MemoryStream()) {
                using (var gzip = new GZipStream(destination, CompressionLevel.Fastest, false)) {
                    gzip.Write(bytes, 0, bytes.Length);
                    gzip.Flush();
                    gzip.Close();
                    return destination.ToArray();
                }
            }
        }

        public static Stream Unzip(this Stream source)
        {
            var destination = new MemoryStream();
            destination.Write(BitConverter.GetBytes(0), 0, SizeBytes); // placeholder for size
            using (var gzip = new GZipStream(source, CompressionMode.Decompress, true)) {
                gzip.CopyTo(destination);
                gzip.Flush();
                gzip.Close();
                //var buffer = new byte[DefaultCopyBufferSize];
                //var totalRead = 0;
                //int read;
                //while ((read = gzip.Read(buffer, 0, length - totalRead)) != 0) {
                //    totalRead += read;
                //    if (totalRead >= length) {
                //        destination.Write(buffer, 0, (int)(length - destination.Length + SizeBytes));
                //        break;
                //    }
                //    destination.Write(buffer, 0, read);
                //}
            }
            destination.Position = 0;
            destination.Write(((int)destination.Length - SizeBytes).ToBytes(), 0, SizeBytes); // fill the placeholder
            destination.Position = 0;
            return destination;
        }

        private const int SizeBytes = 4;

        // From System.IO.Stream:
        // We pick a value that is the largest multiple of 4096 that is still smaller than the large object heap threshold (85K).
        // The CopyTo/CopyToAsync buffer is short-lived and is likely to be collected at Gen0, and it offers a significant
        // improvement in Copy performance.
        private const int DefaultCopyBufferSize = 81920;

    }
}