using System;
using KafkaClient.Protocol;
using NUnit.Framework;

namespace KafkaClient.Tests.Protocol
{
    public static class ProtocolAssertionExtensions
    {
        public static void AssertCanEncodeDecodeRequest<T>(this T request, short version) where T : class, IRequest
        {
            var context = new RequestContext(17, version, "Test-Request");
            var data = KafkaEncoder.EncodeRequestBytes(context, request);
            var decoded = KafkaDecoder.Decode<T>(data);

            if (!request.Equals(decoded)) {
                var original = request.ToFormattedString();
                var final = decoded.ToFormattedString();
                Console.WriteLine($"Original\n{original}\nFinal\n{final}");
                Assert.That(final, Is.EqualTo(original));
                Assert.Fail("Not equal, although strings suggest they are?");
            }
        }

        public static void AssertCanEncodeDecodeResponse<T>(this T response, short version) where T : class, IResponse
        {
            var context = new RequestContext(16, version, "Test-Response");
            var data = KafkaDecoder.EncodeResponseBytes(context, response);
            var decoded = KafkaEncoder.Decode<T>(context, data, true);

            if (!response.Equals(decoded)) {
                var original = response.ToFormattedString();
                var final = decoded.ToFormattedString();
                Console.WriteLine($"Original\n{original}\nFinal\n{final}");
                Assert.That(final, Is.EqualTo(original));
                Assert.Fail("Not equal, although strings suggest they are?");
            }
        }
    }
}