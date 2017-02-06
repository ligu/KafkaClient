using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// ApiVersionsResponse => ErrorCode [ApiKey MinVersion MaxVersion]
    ///  ErrorCode => int16  -- The error code.
    ///  ApiKey => int16     -- The Api Key.
    ///  MinVersion => int16 -- The minimum supported version.
    ///  MaxVersion => int16 -- The maximum supported version.
    ///
    /// From http://kafka.apache.org/protocol.html#protocol_messages
    /// </summary>
    public class ApiVersionsResponse : IResponse, IEquatable<ApiVersionsResponse>
    {
        public override string ToString() => $"{{ErrorCode:{ErrorCode},Apis:[{SupportedVersions.ToStrings()}]}}";

        public ApiVersionsResponse(ErrorCode errorCode = ErrorCode.NONE, IEnumerable<VersionSupport> supportedVersions = null)
        {
            ErrorCode = errorCode;
            Errors = ImmutableList<ErrorCode>.Empty.Add(ErrorCode);
            SupportedVersions = ImmutableList<VersionSupport>.Empty.AddNotNullRange(supportedVersions);
        }

        public IImmutableList<ErrorCode> Errors { get; }

        /// <summary>
        /// The error code.
        /// </summary>
        public ErrorCode ErrorCode { get; }

        public IImmutableList<VersionSupport> SupportedVersions { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as ApiVersionsResponse);
        }

        /// <inheritdoc />
        public bool Equals(ApiVersionsResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return ErrorCode == other.ErrorCode
                && SupportedVersions.HasEqualElementsInOrder(other.SupportedVersions);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                return ((int) ErrorCode*397) ^ (SupportedVersions?.Count.GetHashCode() ?? 0);
            }
        }

        #endregion

        public class VersionSupport : IEquatable<VersionSupport>
        {
            public override string ToString() => $"{{ApiKey:{ApiKey},MinVersion:{MinVersion},MaxVersion:{MaxVersion}}}";

            public VersionSupport(ApiKey apiKey, short minVersion, short maxVersion)
            {
                ApiKey = apiKey;
                MinVersion = minVersion;
                MaxVersion = maxVersion;
            }

            /// <summary>
            /// API key.
            /// </summary>
            public ApiKey ApiKey { get; } 

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
                return Equals(obj as VersionSupport);
            }

            public bool Equals(VersionSupport other)
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

            #endregion

        }
    }
}