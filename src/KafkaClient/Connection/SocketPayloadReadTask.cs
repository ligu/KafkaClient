using System.Threading;

namespace KafkaClient.Connection
{
    internal class SocketPayloadReadTask : SocketPayloadTask<byte[]>
    {
        public SocketPayloadReadTask(int readSize, CancellationToken cancellationToken)
            : base(cancellationToken)
        {
            ReadSize = readSize;
        }

        public int ReadSize { get; }
    }
}