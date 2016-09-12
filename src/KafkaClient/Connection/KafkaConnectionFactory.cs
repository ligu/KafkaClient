using System;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using KafkaClient.Common;

namespace KafkaClient.Connection
{
    public class KafkaConnectionFactory : IKafkaConnectionFactory
    {
        public IKafkaConnection Create(KafkaEndpoint endpoint, TimeSpan responseTimeoutMs, IKafkaLog log, int maxRetry, TimeSpan? maximumReconnectionTimeout = null, StatisticsTrackerOptions statisticsTrackerOptions = null)
        {
            var socket = new KafkaTcpSocket(log, endpoint, maxRetry, maximumReconnectionTimeout, statisticsTrackerOptions);
            return new KafkaConnection(socket, responseTimeoutMs, log);
        }

        public KafkaEndpoint Resolve(Uri kafkaAddress, IKafkaLog log)
        {
            var ipEndpoint = new IPEndPoint(GetFirstAddress(kafkaAddress.Host, log), kafkaAddress.Port);
            return new KafkaEndpoint(kafkaAddress, ipEndpoint);
        }

        private static IPAddress GetFirstAddress(string hostname, IKafkaLog log)
        {
            try {
                var addresses = Dns.GetHostAddresses(hostname);
                if (addresses.Length > 0) {
                    foreach (var address in addresses) {
                        log.DebugFormat("Found address {0} for {1}", address, hostname);
                    }

                    var selectedAddress = addresses.FirstOrDefault(item => item.AddressFamily == AddressFamily.InterNetwork) ?? addresses.First();
                    log.DebugFormat("Using address {0} for {1}", selectedAddress, hostname);
                    return selectedAddress;
                }
            } catch (Exception ex) {
                log.InfoFormat(ex);
            }

            throw new KafkaConnectionException($"Could not resolve the following hostname: {hostname}");
        }
    }
}