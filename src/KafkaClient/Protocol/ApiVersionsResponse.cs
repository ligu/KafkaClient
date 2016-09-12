using System.Collections.Generic;
using System.Collections.Immutable;

namespace KafkaNet.Protocol
{
    public class ApiVersionsResponse : IKafkaResponse
    {
        public ApiVersionsResponse(ErrorResponseCode errorCode = ErrorResponseCode.NoError, IEnumerable<ApiVersionSupport> supportedVersions = null)
        {
            ErrorCode = errorCode;
            Errors = ImmutableList<ErrorResponseCode>.Empty.Add(ErrorCode);
            SupportedVersions = supportedVersions != null ? ImmutableList<ApiVersionSupport>.Empty.AddRange(supportedVersions) : ImmutableList<ApiVersionSupport>.Empty;
        }

        public ImmutableList<ErrorResponseCode> Errors { get; }

        /// <summary>
        /// The error code.
        /// </summary>
        public ErrorResponseCode ErrorCode { get; }

        public ImmutableList<ApiVersionSupport> SupportedVersions { get; }
    }
}