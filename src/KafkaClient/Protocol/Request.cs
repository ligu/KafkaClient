using System;

namespace KafkaClient.Protocol
{
    public abstract class Request : IRequest, IEquatable<Request>
    {
        protected Request(ApiKeyRequestType apiKey, bool expectResponse = true)
        {
            ApiKey = apiKey;
            ExpectResponse = expectResponse;
        }

        /// <summary>
        /// Enum identifying the specific type of request message being represented.
        /// </summary>
        public ApiKeyRequestType ApiKey { get; }

        /// <summary>
        /// Flag which tells the broker call to expect a response for this request.
        /// </summary>
        public bool ExpectResponse { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as Request);
        }

        /// <inheritdoc />
        public bool Equals(Request other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return ApiKey == other.ApiKey && ExpectResponse == other.ExpectResponse;
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                return ((int) ApiKey*397) ^ ExpectResponse.GetHashCode();
            }
        }

        /// <inheritdoc />
        public static bool operator ==(Request left, Request right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(Request left, Request right)
        {
            return !Equals(left, right);
        }
    }
}