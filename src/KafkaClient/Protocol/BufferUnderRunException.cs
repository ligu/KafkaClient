using System;

namespace KafkaClient.Protocol
{
    public class BufferUnderRunException : KafkaException
    {
        public BufferUnderRunException(string message)
            : base(message)
        {
        }

        public BufferUnderRunException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}