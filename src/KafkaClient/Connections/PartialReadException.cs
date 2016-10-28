using System;
using System.Runtime.Serialization;

namespace KafkaClient.Connections
{
    /// <summary>
    /// Only thrown by the <see cref="TcpSocket"/> when a read task fails after partially reading from the stream
    /// </summary>
    [Serializable]
    public class PartialReadException : Exception
    {
        public PartialReadException(int bytesRead, int readSize, Exception innerException)
            : base($"Read {bytesRead} bytes of {readSize} before failing", innerException)
        {
            BytesRead = bytesRead;
            ReadSize = readSize;
        }

        public PartialReadException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            BytesRead = info.GetInt32(nameof(BytesRead));
            ReadSize = info.GetInt32(nameof(ReadSize));
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue(nameof(BytesRead), BytesRead);
            info.AddValue(nameof(ReadSize), ReadSize);
        }


        public int BytesRead { get; set; }
        public int ReadSize { get; set; }
    }
}