using System;
using System.IO.Compression;
using System.Linq;
using BenchmarkDotNet.Attributes;
using KafkaClient.Protocol;

namespace KafkaClient.Performance
{
    public class ProduceBenchmark
    {
        [Params(100, 10000)]
        public int Messages { get; set; }

        [Params(1, 1000)]
        public int MessageSize { get; set; }

        [Params(MessageCodec.CodecGzip)]
        public MessageCodec Codec { get; set; }

        [Params(CompressionLevel.Fastest, CompressionLevel.Optimal)]
        public CompressionLevel Level { get; set; }

        public int Partitions { get; } = 1;

        public short Version { get; } = 0;

        public byte MessageVersion { get; } = 0;

        private ProduceRequest _request;

        [Setup]
        public void SetupData()
        {
            KafkaClient.Common.Compression.ZipLevel = Level;
            _request = new ProduceRequest(
                        Enumerable.Range(1, Partitions)
                                  .Select(partitionId => new ProduceRequest.Payload(
                                      "topic", 
                                      partitionId, 
                                      Enumerable.Range(1, Messages)
                                                .Select(i => new Message(GenerateMessageBytes(), (byte) Codec, version: MessageVersion)), 
                                      Codec)));
        }

        private byte[] GenerateMessageBytes()
        {
            var buffer = new byte[MessageSize];
            new Random(42).NextBytes(buffer);
            return buffer;
        }

        [Benchmark]
        public byte[] Encode()
        {
            return KafkaEncoder.Encode(new RequestContext(1, Version), _request);
        }
    }
}