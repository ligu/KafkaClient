using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using KafkaClient.Common;
using KafkaClient.Connections;
using KafkaClient.Protocol.Types;

namespace KafkaClient.Protocol
{
    public static class Extensions
    {
        public static Exception ExtractExceptions<TResponse>(this IRequest<TResponse> request, TResponse response, Endpoint endpoint = null) where TResponse : IResponse
        {
            var exceptions = new List<Exception>();
            foreach (var errorCode in response.Errors.Where(e => e != ErrorResponseCode.None)) {
                exceptions.Add(ExtractException(request, errorCode, endpoint));
            }
            if (exceptions.Count == 0) return new RequestException(request.ApiKey, ErrorResponseCode.None) { Endpoint = endpoint };
            if (exceptions.Count == 1) return exceptions[0];
            return new AggregateException(exceptions);
        }

        public static Exception ExtractException(this IRequest request, ErrorResponseCode errorCode, Endpoint endpoint) 
        {
            var exception = ExtractFetchException(request as FetchRequest, errorCode) ??
                            new RequestException(request.ApiKey, errorCode);
            exception.Endpoint = endpoint;
            return exception;
        }

        private static FetchOutOfRangeException ExtractFetchException(FetchRequest request, ErrorResponseCode errorCode)
        {
            if (errorCode == ErrorResponseCode.OffsetOutOfRange && request?.Fetches?.Count == 1) {
                var fetch = request.Fetches.First();
                return new FetchOutOfRangeException(fetch, request.ApiKey, errorCode);
            }
            return null;
        } 

        internal static readonly DateTime UnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public static long? ToUnixEpochMilliseconds(this DateTime? pointInTime)
        {
            return pointInTime?.ToUnixEpochMilliseconds();
        }

        public static long ToUnixEpochMilliseconds(this DateTime pointInTime)
        {
            return pointInTime > UnixEpoch ? (long)(pointInTime - UnixEpoch).TotalMilliseconds : 0L;
        }

        public static DateTime FromUnixEpochMilliseconds(this long milliseconds)
        {
            return UnixEpoch.AddMilliseconds(milliseconds);
        }

        public static bool IsSuccess(this ErrorResponseCode code)
        {
            return code == ErrorResponseCode.None;
        }

        /// <summary>
        /// See http://kafka.apache.org/protocol.html#protocol_error_codes for details
        /// </summary>
        public static bool IsRetryable(this ErrorResponseCode code)
        {
            return code == ErrorResponseCode.CorruptMessage
                || code == ErrorResponseCode.UnknownTopicOrPartition
                || code == ErrorResponseCode.LeaderNotAvailable
                || code == ErrorResponseCode.NotLeaderForPartition
                || code == ErrorResponseCode.RequestTimedOut
                || code == ErrorResponseCode.NetworkException
                || code == ErrorResponseCode.GroupLoadInProgress
                || code == ErrorResponseCode.GroupCoordinatorNotAvailable
                || code == ErrorResponseCode.NotCoordinatorForGroup
                || code == ErrorResponseCode.NotEnoughReplicas
                || code == ErrorResponseCode.NotEnoughReplicasAfterAppend;
        }

        public static bool IsFromStaleMetadata(this ErrorResponseCode code)
        {
            return code == ErrorResponseCode.UnknownTopicOrPartition
                || code == ErrorResponseCode.LeaderNotAvailable
                || code == ErrorResponseCode.NotLeaderForPartition
                || code == ErrorResponseCode.GroupLoadInProgress
                || code == ErrorResponseCode.GroupCoordinatorNotAvailable
                || code == ErrorResponseCode.NotCoordinatorForGroup;
        }

        public static string ToFormattedString(this object o)
        {
            return new StringBuilder().AppendWithIndent(o, "").ToString();
        }

        private static StringBuilder AppendWithIndent(this StringBuilder buffer, object o, string indent)
        {
            foreach (var property in o.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance).Where(p => p.CanRead && p.GetIndexParameters().Length <= 1)) {
                buffer.Append($"{indent}{property.Name}: ").AppendValueWithIndent(property.GetValue(o), indent, true);
            }
            return buffer;
        }

        private static StringBuilder AppendValueWithIndent(this StringBuilder buffer, object value, string indent, bool isIndented = false)
        {
            if (value == null) {
                if (!isIndented) {
                    buffer.Append(indent);
                }
                buffer.AppendLine("null");
                return buffer;
            }

            var stringValue = value as string;
            if (stringValue != null) {
                if (!isIndented) {
                    buffer.Append(indent);
                }
                buffer.AppendLine(stringValue);
                return buffer;
            }

            var enumerable = value as IEnumerable;
            if (enumerable != null) {
                buffer.AppendLine("[");
                foreach (var inner in enumerable) {
                    buffer.AppendValueWithIndent(inner, $"{indent}  ");
                }
                buffer.AppendLine($"{indent}]");
                return buffer;
            }

            if (value.GetType().IsClass) {
                buffer.AppendLine();
                buffer.AppendWithIndent(value, $"{indent}");
                return buffer;
            }
            if (!isIndented) {
                buffer.Append(indent);
            }
            buffer.AppendLine(value.ToString());
            return buffer;
        }

        public static IProtocolTypeEncoder GetEncoder(this IRequestContext context, string protocolType = null)
        {
            var type = protocolType ?? context.ProtocolType;
            IProtocolTypeEncoder encoder;
            if (type != null && context.Encoders.TryGetValue(type, out encoder) && encoder != null) return encoder;

            return new ProtocolTypeEncoder();
        }

        public static IKafkaWriter Write(this IKafkaWriter writer, IMemberMetadata metadata, IProtocolTypeEncoder encoder)
        {
            encoder.EncodeMetadata(writer, metadata);
            return writer;
        }

        public static IKafkaWriter Write(this IKafkaWriter writer, IMemberAssignment assignment, IProtocolTypeEncoder encoder)
        {
            encoder.EncodeAssignment(writer, assignment);
            return writer;
        }

        public static IKafkaWriter Write(this IKafkaWriter writer, ErrorResponseCode errorCode)
        {
            return writer.Write((short)errorCode);
        }

        public static ErrorResponseCode ReadErrorCode(this IKafkaReader reader)
        {
            return (ErrorResponseCode) reader.ReadInt16();
        }
    }
}
