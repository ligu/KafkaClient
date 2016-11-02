using KafkaClient.Connections;
using KafkaClient.Protocol;

namespace KafkaClient
{
    public class BrokerRoute : TopicPartition
    {
        public BrokerRoute(string topicName, int partitionId, int brokerId, IConnection connection) : base(topicName, partitionId)
        {
            BrokerId = brokerId;
            Connection = connection;
        }

        public IConnection Connection { get; }

        public int BrokerId { get; }

        public override string ToString() => $"{Connection.Endpoint.ServerUri} ({BrokerId}) {base.ToString()}";

    }
}