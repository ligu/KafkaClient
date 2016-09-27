using System.Threading;
using KafkaClient.Common;

namespace KafkaClient.Connection
{
    internal class SocketPayloadReceiveTask : CancellableTask<byte[]>
    {
        public SocketPayloadReceiveTask(int readSize, CancellationToken cancellationToken)
            : base(cancellationToken)
        {
            ReadSize = readSize;
        }

        public int ReadSize { get; }
    }
}