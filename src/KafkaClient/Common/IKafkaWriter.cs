using System;
using System.Collections.Generic;

namespace KafkaClient.Common
{
    public interface IKafkaWriter : IDisposable
    {
        IKafkaWriter Write(short value);
        IKafkaWriter Write(int value);
        IKafkaWriter Write(long value);

        IKafkaWriter Write(byte value);
        IKafkaWriter Write(byte[] values, bool includePrefix = true);

        IKafkaWriter Write(string value);
        IKafkaWriter Write(IEnumerable<string> values, bool includePrefix = false);

        byte[] ToBytes();
        byte[] ToBytesNoLength();
        byte[] ToBytesCrc();

        IDisposable MarkForLength();
        IDisposable MarkForCrc();
    }
}