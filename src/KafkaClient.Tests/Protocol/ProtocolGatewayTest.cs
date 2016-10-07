using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Protocol;
using KafkaClient.Tests.Helpers;
using NUnit.Framework;

namespace KafkaClient.Tests.Protocol
{
    [TestFixture]
    [Category("Integration")]
    public class ProtocolGatewayTest
    {
        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public async Task ProtocolGateway()
        {
            int partitionId = 0;
            var router = new BrokerRouter(new KafkaOptions(IntegrationConfig.IntegrationUri));

            var producer = new Producer(router);
            string messageValue = Guid.NewGuid().ToString();
            var response = await producer.SendMessageAsync(new Message(messageValue), IntegrationConfig.TopicName(), partitionId, CancellationToken.None);
            var offset = response.Offset;

            var fetch = new Fetch(IntegrationConfig.TopicName(), partitionId, offset, 32000);

            var fetchRequest = new FetchRequest(fetch, minBytes: 10);

            var r = await router.SendAsync(fetchRequest, IntegrationConfig.TopicName(), partitionId, CancellationToken.None);
            //  var r1 = await protocolGateway.SendAsync(fetchRequest, IntegrationConfig.TopicName(), partitionId);
            Assert.IsTrue(r.Topics.First().Messages.First().Value.ToUtf8String() == messageValue);
        }
    }
}