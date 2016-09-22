using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Protocol;
using KafkaClient.Tests.Helpers;
using NUnit.Framework;

namespace KafkaClient.Tests.Integration
{
    [TestFixture]
    [Category("Integration")]
    public class ProtocolGatewayTest
    {
        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task ProtocolGateway()
        {
            int partitionId = 0;
            var router = new BrokerRouter(new KafkaOptions(IntegrationConfig.IntegrationUri));

            var producer = new Producer(router);
            string messageValue = Guid.NewGuid().ToString();
            var response = await producer.SendMessageAsync(new Message(messageValue), IntegrationConfig.IntegrationTopic, partitionId);
            var offset = response.Offset;

            ProtocolGateway protocolGateway = new ProtocolGateway(IntegrationConfig.IntegrationUri);
            var fetch = new Fetch(IntegrationConfig.IntegrationTopic, partitionId, offset, 32000);

            var fetchRequest = new FetchRequest(fetch, minBytes: 10);

            var r = await protocolGateway.SendProtocolRequestAsync(fetchRequest, IntegrationConfig.IntegrationTopic, partitionId, CancellationToken.None);
            //  var r1 = await protocolGateway.SendProtocolRequestAsync(fetchRequest, IntegrationConfig.IntegrationTopic, partitionId);
            Assert.IsTrue(r.Topics.First().Messages.FirstOrDefault().Value.ToUtf8String() == messageValue);
        }
    }
}