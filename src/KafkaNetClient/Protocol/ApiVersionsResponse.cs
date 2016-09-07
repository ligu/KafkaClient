using System.Collections.Generic;
using System.Collections.Immutable;

namespace KafkaNet.Protocol
{
    public class ApiVersionsResponse : IKafkaResponse
    {
        public ApiVersionsResponse(int correlationId, ErrorResponseCode errorCode = ErrorResponseCode.NoError, IEnumerable<ApiVersionSupport> supportedVersions = null)
        {
            CorrelationId = correlationId;
            ErrorCode = errorCode;
            Errors = ImmutableList<ErrorResponseCode>.Empty.Add(ErrorCode);
            SupportedVersions = supportedVersions != null ? ImmutableList<ApiVersionSupport>.Empty.AddRange(supportedVersions) : ImmutableList<ApiVersionSupport>.Empty;
        }

        /// <summary>
        /// Request Correlation
        /// </summary>
        public int CorrelationId { get; }

        public ImmutableList<ErrorResponseCode> Errors { get; }

        /// <summary>
        /// The error code.
        /// </summary>
        public ErrorResponseCode ErrorCode { get; }

        public ImmutableList<ApiVersionSupport> SupportedVersions { get; }
    }
}