using System.Threading;

namespace KafkaClient.Connection
{
    internal class SocketPayloadSendTask : SocketPayloadTask<DataPayload>
    {
        public DataPayload Payload { get; }

        public SocketPayloadSendTask(DataPayload payload, CancellationToken cancellationToken)
            : base(cancellationToken)
        {
            Payload = payload;
        }
    }
}