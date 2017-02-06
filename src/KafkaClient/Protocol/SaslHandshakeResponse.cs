using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// SaslHandshake Response (Version: 0) => error_code [enabled_mechanisms] 
    ///   error_code => INT16
    ///   enabled_mechanisms => STRING
    /// </summary>
    public class SaslHandshakeResponse : IResponse, IEquatable<SaslHandshakeResponse>
    {
        public override string ToString() => $"{{ErrorCode:{ErrorCode},EnabledMechanisms:[{EnabledMechanisms.ToStrings()}]}}";

        public SaslHandshakeResponse(ErrorCode errorCode, IEnumerable<string> enabledMechanisms = null)
        {
            ErrorCode = errorCode;
            Errors = ImmutableList<ErrorCode>.Empty.Add(ErrorCode);
            EnabledMechanisms = ImmutableList<string>.Empty.AddNotNullRange(enabledMechanisms);
        }

        /// <inheritdoc />
        public IImmutableList<ErrorCode> Errors { get; }

        public ErrorCode ErrorCode { get; }

        /// <summary>
        /// Array of mechanisms enabled in the server.
        /// </summary>
        public IImmutableList<string> EnabledMechanisms { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as SaslHandshakeResponse);
        }

        /// <inheritdoc />
        public bool Equals(SaslHandshakeResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return ErrorCode == other.ErrorCode 
                   && EnabledMechanisms.HasEqualElementsInOrder(other.EnabledMechanisms);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                return ((int) ErrorCode*397) ^ (EnabledMechanisms?.Count.GetHashCode() ?? 0);
            }
        }
        
        #endregion
    }
}