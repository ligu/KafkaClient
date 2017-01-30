namespace KafkaClient.Protocol
{
    /// <summary>
    /// Enumeration which specifies the compression type of messages
    /// </summary>
    public enum MessageCodec : byte
    {
        None = 0x00,
        Gzip = 0x01,
        Snappy = 0x02
    }
}