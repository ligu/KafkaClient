using KafkaClient.Common;

namespace KafkaClient.Connection
{
    public static class Extensions
    {
        public static IKafkaConnection Create(this KafkaOptions options, KafkaEndpoint endpoint)
        {
            return options.KafkaConnectionFactory.Create(endpoint, options, options.Log);
        }

        public static IKafkaConnection Create(this IKafkaConnectionFactory factory, KafkaEndpoint endpoint, IKafkaConnectionOptions options, IKafkaLog log)
        {
            return factory.Create(
                endpoint,
                options.RequestTimeout,
                log,
                options.MaxRetries,
                options.ConnectingTimeout,
                options.TrackTelemetry);
        }
    }
}