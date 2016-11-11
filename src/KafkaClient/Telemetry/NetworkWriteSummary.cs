using KafkaClient.Connections;

namespace KafkaClient.Telemetry
{
    public class NetworkWriteSummary
    {
        public Endpoint Endpoint;

        public NetworkTcpSummary TcpSummary = new NetworkTcpSummary();
        public NetworkQueueSummary QueueSummary = new NetworkQueueSummary();
    }
}