using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.Serialization;
using KafkaClient.Common;

namespace KafkaClient.Connection
{
    [Serializable]
    public class Endpoint : IEquatable<Endpoint>
    {
        public Endpoint(Uri serverUri, IPEndPoint ip)
        {
            ServerUri = serverUri;
            IP = ip;
        }

        [SuppressMessage("ReSharper", "UnusedParameter.Local")]
        public Endpoint(SerializationInfo info, StreamingContext context)
        {
            ServerUri = info.GetValue<Uri>("ServerUri");
            IP = info.GetValue<IPEndPoint>("IP");
        }

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("ServerUri", ServerUri);
            info.AddValue("IP", IP);
        }

        public Uri ServerUri { get; }
        [SuppressMessage("ReSharper", "InconsistentNaming")]
        public IPEndPoint IP { get; }

        #region Equality

        public override bool Equals(object obj)
        {
            return Equals(obj as Endpoint);
        }

        public bool Equals(Endpoint other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Equals(IP, other.IP);
        }

        public override int GetHashCode()
        {
            // calculated like this to ensure ports on same address sort in descending order
            return IP?.Address.GetHashCode() + IP?.Port ?? 0;
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

        public override string ToString() => ServerUri.ToString();

        public static Endpoint Resolve(Uri serverUri, ILog log)
        {
            var ipEndpoint = new IPEndPoint(GetFirstAddress(serverUri.Host, log), serverUri.Port);
            return new Endpoint(serverUri, ipEndpoint);
        }

        private static IPAddress GetFirstAddress(string hostname, ILog log)
        {
            try {
                var addresses = Dns.GetHostAddresses(hostname);
                if (addresses.Length > 0) {
                    foreach (var address in addresses) {
                        log?.DebugFormat("Found address {0} for {1}", address, hostname);
                    }

                    var selectedAddress = addresses.FirstOrDefault(item => item.AddressFamily == AddressFamily.InterNetwork) ?? addresses.First();
                    log?.DebugFormat("Using address {0} for {1}", selectedAddress, hostname);
                    return selectedAddress;
                }
            } catch (Exception ex) {
                log?.Info(() => LogEvent.Create(ex));
            }

            throw new ConnectionException($"Could not resolve the following hostname: {hostname}");
        }
    }
}