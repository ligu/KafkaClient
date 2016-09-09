using KafkaNet.Model;
using System;
using System.Linq;
using System.Net;
using System.Net.Sockets;

namespace KafkaNet
{
    public class DefaultKafkaConnectionFactory : IKafkaConnectionFactory
    {
        public IKafkaConnection Create(KafkaEndpoint endpoint, TimeSpan responseTimeoutMs, IKafkaLog log, int maxRetry, TimeSpan? maximumReconnectionTimeout = null, StatisticsTrackerOptions statisticsTrackerOptions = null)
        {
            KafkaTcpSocket socket = new KafkaTcpSocket(log, endpoint, maxRetry, maximumReconnectionTimeout, statisticsTrackerOptions);
            return new KafkaConnection(socket, responseTimeoutMs, log);
        }

        public KafkaEndpoint Resolve(Uri kafkaAddress, IKafkaLog log)
        {
            var ipAddress = GetFirstAddress(kafkaAddress.Host, log);
            var ipEndpoint = new IPEndPoint(ipAddress, kafkaAddress.Port);

            var kafkaEndpoint = new KafkaEndpoint(kafkaAddress, ipEndpoint);
            return kafkaEndpoint;
        }

        private static IPAddress GetFirstAddress(string hostname, IKafkaLog log)
        {
            try
            {
                //lookup the IP address from the provided host name
                var addresses = Dns.GetHostAddresses(hostname);

                if (addresses.Length > 0)
                {
                    Array.ForEach(addresses, address => log.DebugFormat("Found address {0} for {1}", address, hostname));

                    var selectedAddress = addresses.FirstOrDefault(item => item.AddressFamily == AddressFamily.InterNetwork) ?? addresses.First();

                    log.DebugFormat("Using address {0} for {1}", selectedAddress, hostname);

                    return selectedAddress;
                }
            }
            catch
            {
                throw new KafkaConnectionException($"Could not resolve the following hostname: {hostname}");
            }

            throw new KafkaConnectionException($"Could not resolve the following hostname: {hostname}");
        }
    }
}