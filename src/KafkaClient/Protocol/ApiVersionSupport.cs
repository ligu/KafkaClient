using System;

namespace KafkaNet.Protocol
{
    public class ApiVersionSupport : IEquatable<ApiVersionSupport>
    {
        public ApiVersionSupport(ApiKeyRequestType apiKey, short minVersion, short maxVersion)
        {
            ApiKey = apiKey;
            MinVersion = minVersion;
            MaxVersion = maxVersion;
        }

        /// <summary>
        /// API key.
        /// </summary>
        public ApiKeyRequestType ApiKey { get; } 

        /// <summary>
        /// Minimum supported version.
        /// </summary>
        public short MinVersion { get; }

        /// <summary>
        /// Maximum supported version.
        /// </summary>
        public short MaxVersion { get; }

        #region Equality

        public override bool Equals(object obj)
        {
            return Equals(obj as ApiVersionSupport);
        }

        public bool Equals(ApiVersionSupport other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return ApiKey == other.ApiKey && MinVersion == other.MinVersion && MaxVersion == other.MaxVersion;
        }

        public override int GetHashCode()
        {
            unchecked {
                var hashCode = (int) ApiKey;
                hashCode = (hashCode*397) ^ MinVersion.GetHashCode();
                hashCode = (hashCode*397) ^ MaxVersion.GetHashCode();
                return hashCode;
            }
        }

        public static bool operator ==(ApiVersionSupport left, ApiVersionSupport right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(ApiVersionSupport left, ApiVersionSupport right)
        {
            return !Equals(left, right);
        }

        #endregion

    }
}