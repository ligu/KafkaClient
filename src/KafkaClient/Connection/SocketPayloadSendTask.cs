using System.Threading;

namespace KafkaClient.Connection
{
    internal class SocketPayloadSendTask : SocketPayloadTask<KafkaDataPayload>
    {
        public KafkaDataPayload Payload { get; }

        public SocketPayloadSendTask(KafkaDataPayload payload, CancellationToken cancellationToken)
            : base(cancellationToken)
        {
            Payload = payload;
        }
    }
}