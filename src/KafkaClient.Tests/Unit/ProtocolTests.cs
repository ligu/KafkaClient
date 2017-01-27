using KafkaClient.Assignment;
using KafkaClient.Common;
using KafkaClient.Protocol;
using NUnit.Framework;

namespace KafkaClient.Tests.Unit
{
    [TestFixture]
    public class ProtocolTests
    {
        [Test]
        public void MetadataResponseShouldDecode()
        {
            var response = KafkaEncoder.Decode<MetadataResponse>(new RequestContext(1), ApiKey.Metadata, MessageHelper.CreateMetadataResponse(new RequestContext(1), "Test").Skip(KafkaEncoder.ResponseHeaderSize));

            Assert.That(response.Topics[0].TopicName, Is.EqualTo("Test"));
        }

        [Test]
        public void InterfacesAreFormattedWithinProtocol()
        {
            var request = new SyncGroupRequest("group", 5, "member", new []{ new SyncGroupRequest.GroupAssignment("member", new ConsumerMemberAssignment(new []{ new TopicPartition("topic", 0), new TopicPartition("topic", 1) }))});
            var formatted = request.ToString();
            Assert.That(formatted.Contains("TopicName:topic"));
            Assert.That(formatted.Contains("PartitionId:1"));
        }
    }
}