using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    public class ApiVersionsResponse : IResponse
    {
        public ApiVersionsResponse(ErrorResponseCode errorCode = ErrorResponseCode.NoError, IEnumerable<ApiVersionSupport> supportedVersions = null)
        {
            ErrorCode = errorCode;
            Errors = ImmutableList<ErrorResponseCode>.Empty.Add(ErrorCode);
            SupportedVersions = ImmutableList<ApiVersionSupport>.Empty.AddNotNullRange(supportedVersions);
        }

        public ImmutableList<ErrorResponseCode> Errors { get; }

        /// <summary>
        /// The error code.
        /// </summary>
        public ErrorResponseCode ErrorCode { get; }

        public ImmutableList<ApiVersionSupport> SupportedVersions { get; }
    }
}