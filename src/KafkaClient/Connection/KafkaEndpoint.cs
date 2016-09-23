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
    public class KafkaEndpoint : IEquatable<KafkaEndpoint>
    {
        public KafkaEndpoint(Uri serverUri, IPEndPoint endpoint)
        {
            ServerUri = serverUri;
            Endpoint = endpoint;
        }

        [SuppressMessage("ReSharper", "UnusedParameter.Local")]
        public KafkaEndpoint(SerializationInfo info, StreamingContext context)
        {
            ServerUri = info.GetValue<Uri>("ServerUri");
            Endpoint = info.GetValue<IPEndPoint>("Endpoint");
        }

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("ServerUri", ServerUri);
            info.AddValue("Endpoint", Endpoint);
        }

        public Uri ServerUri { get; }
        public IPEndPoint Endpoint { get; }

        #region Equality

        public override bool Equals(object obj)
        {
            return Equals(obj as KafkaEndpoint);
        }

        public bool Equals(KafkaEndpoint other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Equals(Endpoint, other.Endpoint);
        }

        public override int GetHashCode()
        {
            // calculated like this to ensure ports on same address sort in descending order
            return Endpoint?.Address.GetHashCode() + Endpoint?.Port ?? 0;
        }

        public static bool operator ==(KafkaEndpoint left, KafkaEndpoint right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(KafkaEndpoint left, KafkaEndpoint right)
        {
            return !Equals(left, right);
        }

        #endregion

        public override string ToString() => ServerUri.ToString();

        public static KafkaEndpoint Resolve(Uri serverUri, IKafkaLog log)
        {
            var ipEndpoint = new IPEndPoint(GetFirstAddress(serverUri.Host, log), serverUri.Port);
            return new KafkaEndpoint(serverUri, ipEndpoint);
        }

        private static IPAddress GetFirstAddress(string hostname, IKafkaLog log)
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
                log?.InfoFormat(ex);
            }

            throw new KafkaConnectionException($"Could not resolve the following hostname: {hostname}");
        }
    }
}