namespace KafkaClient.Connection
{
    public static class Extensions
    {
        public static IKafkaConnection Create(this KafkaOptions options, KafkaEndpoint endpoint)
        {
            return options.ConnectionFactory.Create(endpoint, options.ConnectionOptions, options.Log);
        }
    }
}