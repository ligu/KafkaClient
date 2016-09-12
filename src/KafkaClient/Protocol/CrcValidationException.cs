using System;
using System.Runtime.Serialization;

namespace KafkaClient.Protocol
{
    [Serializable]
    public class CrcValidationException : KafkaException
    {
        public CrcValidationException(uint crc, uint calculatedCrc)
            : base("Calculated CRC did not match reported CRC.")
        {
            Crc = crc;
            CalculatedCrc = calculatedCrc;
        }

        public CrcValidationException(string message)
            : base(message)
        {
        }

        public CrcValidationException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public CrcValidationException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            Crc = info.GetUInt32("Crc");
            CalculatedCrc = info.GetUInt32("CalculatedCrc");
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue("Crc", Crc);
            info.AddValue("CalculatedCrc", CalculatedCrc);
        }

        public uint Crc { get; set; }
        public uint CalculatedCrc { get; set; }
    }
}