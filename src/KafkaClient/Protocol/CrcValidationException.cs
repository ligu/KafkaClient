namespace KafkaClient.Protocol
{
    public class CrcValidationException : KafkaException
    {
        public CrcValidationException(uint crc, uint crcHash)
            : base($"CRC {crc} and hashed content {crcHash} did not match.")
        {
            _crc = crc;
            _crcHash = crcHash;
        }

        // ReSharper disable NotAccessedField.Local -- for debugging
        private readonly uint _crc;
        private readonly uint _crcHash;
        // ReSharper restore NotAccessedField.Local
    }
}