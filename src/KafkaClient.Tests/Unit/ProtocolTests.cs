using System.Linq;
using KafkaClient.Protocol;
using KafkaClient.Tests.Helpers;
using NUnit.Framework;

namespace KafkaClient.Tests.Unit
{
    [TestFixture]
    [Category("Unit")]
    public class ProtocolTests
    {
        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void MetadataResponseShouldDecode()
        {
            var response = KafkaEncoder.Decode<MetadataResponse>(new RequestContext(1), MessageHelper.CreateMetadataResponse(1, "Test").Skip(4).ToArray());

            //Assert.That(response.CorrelationId, Is.EqualTo(1));
            Assert.That(response.Topics[0].TopicName, Is.EqualTo("Test"));
        }
    }
}