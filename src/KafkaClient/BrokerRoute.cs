using KafkaClient.Connection;
using KafkaClient.Protocol;

namespace KafkaClient
{
    public class BrokerRoute : Topic
    {
        public BrokerRoute(string topicName, int partitionId, IKafkaConnection connection) : base(topicName, partitionId)
        {
            Connection = connection;
        }

        public IKafkaConnection Connection { get; }

        public override string ToString() => $"{Connection.Endpoint.ServerUri} {base.ToString()}";

    }
}