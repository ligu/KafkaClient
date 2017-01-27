using System;
using System.IO.Compression;
using System.Linq;
using BenchmarkDotNet.Attributes;
using KafkaClient.Common;
using KafkaClient.Protocol;
using KafkaClient.Testing;

namespace KafkaClient.Performance
{
    public class FetchBenchmark
    {
        [Params(100, 10000)]
        public int Messages { get; set; }

        [Params(1, 1000)]
        public int MessageSize { get; set; }

        [Params(MessageCodec.CodecNone, MessageCodec.CodecGzip)]
        public MessageCodec Codec { get; set; }

        [Params(CompressionLevel.Optimal)]
        public CompressionLevel Level { get; set; }

        public int Partitions { get; } = 1;

        public short Version { get; } = 0;

        public byte MessageVersion { get; } = 0;

        private ArraySegment<byte> _bytes;

        [Setup]
        public void SetupData()
        {
            Common.Compression.ZipLevel = Level;
            var response = new FetchResponse(
                Enumerable.Range(1, Partitions)
                          .Select(partitionId => new FetchResponse.Topic(
                              "topic", 
                              partitionId, 
                              500,
                              ErrorCode.None,
                              Enumerable.Range(1, Messages)
                                        .Select(i => new Message(GenerateMessageBytes(), new ArraySegment<byte>(), (byte) Codec, version: MessageVersion))
                          )));
            _bytes = KafkaDecoder.EncodeResponseBytes(new RequestContext(1, Version), response);
        }

        private ArraySegment<byte> GenerateMessageBytes()
        {
            var buffer = new byte[MessageSize];
            new Random(42).NextBytes(buffer);
            return new ArraySegment<byte>(buffer);
        }

        [Benchmark]
        public FetchResponse Decode()
        {
            return KafkaEncoder.Decode<FetchResponse>(new RequestContext(1, Version), ApiKey.Fetch, _bytes.Skip(KafkaEncoder.ResponseHeaderSize));
        }
    }
}