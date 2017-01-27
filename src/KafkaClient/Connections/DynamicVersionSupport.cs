using KafkaClient.Protocol;

namespace KafkaClient.Connections
{
    public class DynamicVersionSupport : IVersionSupport
    {
        private readonly VersionSupport _defaultSupport;

        public DynamicVersionSupport(VersionSupport defaultSupport)
        {
            _defaultSupport = defaultSupport;
        }

        public bool UseMaxSupported { get; set; }

        public short? GetVersion(ApiKeyRequestType apiKey) => _defaultSupport.GetVersion(apiKey);
    }
}