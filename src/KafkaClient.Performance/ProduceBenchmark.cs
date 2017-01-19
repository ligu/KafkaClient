using System.Collections.Generic;
using System.Linq;
using System.Text;
using BenchmarkDotNet.Attributes;
using KafkaClient.Protocol;

namespace KafkaClient.Performance
{
    public class ProduceBenchmark
    {
        [Params(100, 1000)]
        public int Messages { get; set; }

        [Params(MessageCodec.CodecNone, MessageCodec.CodecGzip)]
        public MessageCodec Codec { get; set; }

        public int Partitions { get; } = 1;

        public short Version { get; } = 0;

        public byte MessageVersion { get; } = 0;

        private ProduceRequest _request;

        [Setup]
        public void SetupData()
        {
            _request = new ProduceRequest(
                        Enumerable.Range(1, Partitions)
                                  .Select(partitionId => new ProduceRequest.Payload(
                                      "topic", 
                                      partitionId, 
                                      Enumerable.Range(1, Messages)
                                                .Select(i => new Message(Encoding.UTF8.GetBytes(i.ToString()), (byte) Codec, version: MessageVersion)), 
                                      Codec)));
        }

        [Benchmark]
        public byte[] Encode()
        {
            return KafkaEncoder.Encode(new RequestContext(1, Version), _request);
        }
    }
}