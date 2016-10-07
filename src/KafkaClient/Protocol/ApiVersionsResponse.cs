using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    public class ApiVersionsResponse : IResponse, IEquatable<ApiVersionsResponse>
    {
        public ApiVersionsResponse(ErrorResponseCode errorCode = ErrorResponseCode.None, IEnumerable<ApiVersionSupport> supportedVersions = null)
        {
            ErrorCode = errorCode;
            Errors = ImmutableList<ErrorResponseCode>.Empty.Add(ErrorCode);
            SupportedVersions = ImmutableList<ApiVersionSupport>.Empty.AddNotNullRange(supportedVersions);
        }

        public IImmutableList<ErrorResponseCode> Errors { get; }

        /// <summary>
        /// The error code.
        /// </summary>
        public ErrorResponseCode ErrorCode { get; }

        public IImmutableList<ApiVersionSupport> SupportedVersions { get; }

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
                return ((int) ErrorCode*397) ^ (SupportedVersions?.GetHashCode() ?? 0);
            }
        }

        /// <inheritdoc />
        public static bool operator ==(ApiVersionsResponse left, ApiVersionsResponse right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(ApiVersionsResponse left, ApiVersionsResponse right)
        {
            return !Equals(left, right);
        }
    }
}