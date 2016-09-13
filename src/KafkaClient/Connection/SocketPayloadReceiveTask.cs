using System.Threading;

namespace KafkaClient.Connection
{
    internal class SocketPayloadReceiveTask : SocketPayloadTask<byte[]>
    {
        public SocketPayloadReceiveTask(int readSize, CancellationToken cancellationToken)
            : base(cancellationToken)
        {
            ReadSize = readSize;
        }

        public int ReadSize { get; }
    }
}