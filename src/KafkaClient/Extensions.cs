using KafkaClient.Connections;

namespace KafkaClient
{
    public static class Extensions
    {
        #region Configuration helpers

        public static IVersionSupport Dynamic(this VersionSupport versionSupport)
        {
            return new DynamicVersionSupport(versionSupport);
        }

        #endregion

        #region KafkaOptions helpers

        public static IConnection CreateConnection(this KafkaOptions options, Endpoint endpoint)
        {
            return options.ConnectionFactory.Create(endpoint, options.ConnectionConfiguration, options.Log);
        }

        #endregion
    }
}