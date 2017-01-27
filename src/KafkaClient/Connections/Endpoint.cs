using System;
using System.Net;
using System.Net.Sockets;
using KafkaClient.Common;
using System.Linq;
using System.Threading.Tasks;

namespace KafkaClient.Connections
{
    public class Endpoint : IEquatable<Endpoint>
    {
        public Endpoint(IPEndPoint ip, string host = null)
        {
            Ip = ip;
            Host = host ?? ip.Address.ToString();
        }


        public string Host { get; }
        public IPEndPoint Ip { get; }

        public static implicit operator IPEndPoint(Endpoint endpoint)
        {
            return endpoint.Ip;
        }

        #region Equality

        public override bool Equals(object obj)
        {
            return Equals(obj as Endpoint);
        }

        public bool Equals(Endpoint other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Equals(Ip, other.Ip);
        }

        public override int GetHashCode()
        {
            // calculated like this to ensure ports on same address sort in descending order
            return Ip?.Address.GetHashCode() + Ip?.Port ?? 0;
        }

        public static bool operator ==(Endpoint left, Endpoint right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(Endpoint left, Endpoint right)
        {
            return !Equals(left, right);
        }

        #endregion

        public override string ToString() => $"http://{Host}:{Ip?.Port}";

        public static async Task<Endpoint> ResolveAsync(Uri uri, ILog log)
        {
            var ipAddress = await GetFirstAddress(uri.Host, log);
            var ipEndpoint = new IPEndPoint(ipAddress, uri.Port);
            return new Endpoint(ipEndpoint, uri.DnsSafeHost);
        }

        private static async Task<IPAddress> GetFirstAddress(string hostname, ILog log)
        {
            try {
                var addresses = await Dns.GetHostAddressesAsync(hostname);
                if (addresses.Length > 0) {
                    foreach (var address in addresses) {
                        log?.Verbose(() => LogEvent.Create($"Found address {address} for {hostname}"));
                    }

                    var selectedAddress = addresses.FirstOrDefault(item => item.AddressFamily == AddressFamily.InterNetwork) ?? addresses.First();
                    log?.Debug(() => LogEvent.Create($"Using address {selectedAddress} for {hostname}"));
                    return selectedAddress;
                }
            } catch (Exception ex) {
                log?.Info(() => LogEvent.Create(ex));
            }

            throw new ConnectionException($"Could not resolve the following hostname: {hostname}");
        }
    }
}