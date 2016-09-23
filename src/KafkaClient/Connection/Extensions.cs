namespace KafkaClient.Connection
{
    public static class Extensions
    {
        public static IConnection Create(this KafkaOptions options, Endpoint endpoint)
        {
            return options.ConnectionFactory.Create(endpoint, options.ConnectionConfiguration, options.Log);
        }
    }
}