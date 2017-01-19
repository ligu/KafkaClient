using System.Linq;
using System.Text;
using BenchmarkDotNet.Attributes;
using KafkaClient.Protocol;

namespace KafkaClient.Performance
{
    public class FetchBenchmark
    {
        [Params(100, 1000, 10000)]
        public int Messages { get; set; }

        [Params(MessageCodec.CodecNone, MessageCodec.CodecGzip)]
        public MessageCodec Codec { get; set; }

        public int Partitions { get; } = 1;

        public short Version { get; } = 0;

        public byte MessageVersion { get; } = 0;

        private byte[] _bytes;

        [Setup]
        public void SetupData()
        {
            var response = new FetchResponse(
                Enumerable.Range(1, Partitions)
                          .Select(partitionId => new FetchResponse.Topic(
                              "topic", 
                              partitionId, 
                              500,
                              ErrorResponseCode.None,
                              Enumerable.Range(1, Messages)
                                        .Select(i => new Message(Encoding.UTF8.GetBytes(i.ToString()), (byte) Codec, version: MessageVersion))
                          )));
            _bytes = KafkaDecoder.EncodeResponseBytes(new RequestContext(1, Version), response);
        }

        [Benchmark]
        public FetchResponse Encode()
        {
            return KafkaEncoder.Decode<FetchResponse>(new RequestContext(1, Version), ApiKeyRequestType.Fetch, _bytes);
        }
    }
}