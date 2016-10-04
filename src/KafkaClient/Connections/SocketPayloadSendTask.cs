using System.Threading;
using KafkaClient.Common;

namespace KafkaClient.Connections
{
    internal class SocketPayloadSendTask : CancellableTask<DataPayload>
    {
        public DataPayload Payload { get; }

        public SocketPayloadSendTask(DataPayload payload, CancellationToken cancellationToken)
            : base(cancellationToken)
        {
            Payload = payload;
        }
    }
}