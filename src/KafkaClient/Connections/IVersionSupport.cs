using KafkaClient.Protocol;

namespace KafkaClient.Connections
{
    public interface IVersionSupport
    {
        short? GetVersion(ApiKey apiKey);
    }
}