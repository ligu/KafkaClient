using System;
using System.Net;
using System.Runtime.Serialization;

namespace KafkaNet.Model
{
    [Serializable]
    public class KafkaEndpoint
    {
        public Uri ServeUri { get; }
        public IPEndPoint Endpoint { get; }

        protected bool Equals(KafkaEndpoint other)
        {
            if (ReferenceEquals(null, other)) return false;
            return Equals(Endpoint, other.Endpoint);
        }

        public override int GetHashCode()
        {
            // calculated like this to ensure ports on same address sort in the desc order
            return Endpoint?.Address.GetHashCode() + Endpoint?.Port ?? 0;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            return Equals(obj as KafkaEndpoint);
        }

        public override string ToString()
        {
            return ServeUri.ToString();
        }

        public KafkaEndpoint(Uri serverUri, IPEndPoint endpoint)
        {
            ServeUri = serverUri;
            Endpoint = endpoint;
        }

        public KafkaEndpoint(SerializationInfo info, StreamingContext context)
        {
            ServeUri = info.GetValue<Uri>("ServeUri");
            Endpoint = info.GetValue<IPEndPoint>("Endpoint");
        }

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("ServeUri", ServeUri);
            info.AddValue("Endpoint", Endpoint);
        }
    }
}