using System;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using KafkaClient.Connections;
using KafkaClient.Protocol;
using Nito.AsyncEx;

namespace KafkaClient.Performance
{
    public class ProduceRequestBenchmark
    {
        [Params(100, 1000)]
        public int Messages { get; set; }

        [Params(1000)]
        public int MessageSize { get; set; }

        [Params(MessageCodec.CodecGzip)]
        public MessageCodec Codec { get; set; }

        [Params(CompressionLevel.Fastest, CompressionLevel.Optimal)]
        public CompressionLevel Level { get; set; }

        public int Partitions { get; } = 5;

        public short Version { get; } = 0;

        public byte MessageVersion { get; } = 0;

        private ProduceRequest _request;

        private TcpServer _server;
        private Connection _connection;

        [Setup]
        public void SetupData()
        {
            Common.Compression.ZipLevel = Level;
            _request = new ProduceRequest(
                Enumerable.Range(1, Partitions)
                          .Select(partitionId => new ProduceRequest.Payload(
                              "topic", 
                              partitionId, 
                              Enumerable.Range(1, Messages)
                                        .Select(i => new Message(GenerateMessageBytes(), (byte) Codec, version: MessageVersion)), 
                              Codec)));

            var response = new ProduceResponse(new ProduceResponse.Topic("topic", 1, ErrorResponseCode.None, 0));

            var port = 10000;
            var endpoint = new Endpoint(new IPEndPoint(IPAddress.Loopback, port), "localhost");
            _server = new TcpServer(endpoint.Value.Port) {
                OnBytesRead = b =>
                {
                    var header = KafkaDecoder.DecodeHeader(b.Array);
                    var bytes = KafkaDecoder.EncodeResponseBytes(new RequestContext(header.CorrelationId), response);
                    AsyncContext.Run(async () => await _server.WriteBytesAsync(new ArraySegment<byte>(bytes)));
                }
            };
            _connection = new Connection(endpoint);
        }

        private byte[] GenerateMessageBytes()
        {
            var buffer = new byte[MessageSize];
            new Random(42).NextBytes(buffer);
            return buffer;
        }

        [Benchmark]
        public async Task<ProduceResponse> EncodeAndSend()
        {
            return await _connection.SendAsync(_request, CancellationToken.None);
        }

    }
}