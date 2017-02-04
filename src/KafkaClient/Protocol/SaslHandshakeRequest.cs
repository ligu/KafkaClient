using System;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// SaslHandshake Request (Version: 0) => mechanism 
    ///   mechanism => STRING
    /// </summary>
    public class SaslHandshakeRequest : Request, IRequest<SaslHandshakeResponse>, IEquatable<SaslHandshakeRequest>
    {
        public override string ToString() => $"{{Api:{ApiKey},Mechanism:{Mechanism}}}";

        public SaslHandshakeRequest(string mechanism)
            : base(ApiKey.SaslHandshake)
        {
            Mechanism = mechanism;
        }

        /// <summary>
        /// SASL Mechanism chosen by the client.
        /// </summary>
        public string Mechanism { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as SaslHandshakeRequest);
        }

        /// <inheritdoc />
        public bool Equals(SaslHandshakeRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) && string.Equals(Mechanism, other.Mechanism);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                return (base.GetHashCode()*397) ^ (Mechanism?.GetHashCode() ?? 0);
            }
        }
        
        #endregion
    }
}