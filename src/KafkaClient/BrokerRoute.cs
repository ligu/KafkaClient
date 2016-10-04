using KafkaClient.Connections;
using KafkaClient.Protocol;

namespace KafkaClient
{
    public class BrokerRoute : Topic
    {
        public BrokerRoute(string topicName, int partitionId, IConnection connection) : base(topicName, partitionId)
        {
            Connection = connection;
        }

        public IConnection Connection { get; }

        public override string ToString() => $"{Connection.Endpoint.ServerUri} {base.ToString()}";

    }
}