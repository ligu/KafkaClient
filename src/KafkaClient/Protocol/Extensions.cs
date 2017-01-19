using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Reflection;
using System.Text;
using KafkaClient.Assignment;
using KafkaClient.Common;
using KafkaClient.Connections;

namespace KafkaClient.Protocol
{
    public static class Extensions
    {
        public static Exception ExtractExceptions<TResponse>(this IRequest<TResponse> request, TResponse response, Endpoint endpoint = null) where TResponse : IResponse
        {
            var exceptions = new List<Exception>();
            if (response != null) {
                foreach (var errorCode in response.Errors.Where(e => e != ErrorResponseCode.None)) {
                    exceptions.Add(ExtractException(request, errorCode, endpoint));
                }
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
            if (errorCode == ErrorResponseCode.OffsetOutOfRange && request?.Topics?.Count == 1) {
                var fetch = request.Topics.First();
                return new FetchOutOfRangeException(fetch, request.ApiKey, errorCode);
            }
            return null;
        } 

        public static long? ToUnixTimeMilliseconds(this DateTimeOffset? pointInTime)
        {
            return pointInTime?.ToUnixTimeMilliseconds();
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
                || code == ErrorResponseCode.NotEnoughReplicasAfterAppend
                || code == ErrorResponseCode.NotController;
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
            return new StringBuilder().AppendObject(o).ToString();
        }

        private static StringBuilder AppendWithSeparator(this StringBuilder buffer, IEnumerable<Action<StringBuilder>> things, string separator = ",")
        {
            var needsSeparator = false;
            foreach (var thing in things) {
                if (needsSeparator) {
                    buffer.Append(separator);
                }
                thing(buffer);
                needsSeparator = true;
            }
            return buffer;
        }

        private static StringBuilder AppendObject(this StringBuilder buffer, object o)
        {
            var properties = o.GetType().GetRuntimeProperties().Where(p => p.CanRead && p.GetIndexParameters().Length <= 1);
            var writers = properties.Select<PropertyInfo, Action<StringBuilder>>(property => b => b.Append(property.Name).Append(":").AppendValue(property.GetValue(o)));
            return buffer.Append("{").AppendWithSeparator(writers).Append("}");
        }

        private static void AppendValue(this StringBuilder buffer, object value)
        {
            if (value == null) {
                buffer.Append("null");
                return;
            }
            if (value is string) {
                buffer.Append($"'{value}'");
                return;
            }

            var bytes = value as byte[];
            if (bytes != null) {
                if (bytes.Length == 0) {
                    buffer.Append("[]");
                } else {
                    buffer.Append("{length:").Append(bytes.Length).Append("}");
                }
                return;
            }

            var enumerable = value as IEnumerable;
            if (enumerable != null) {
                buffer.Append("[").AppendWithSeparator(enumerable.Cast<object>().Select<object, Action<StringBuilder>>(o => b => b.AppendValue(o))).Append("]");
            } else if (value.GetType().GetTypeInfo().IsClass) {
                buffer.AppendObject(value);
            } else {
                buffer.Append(value);
            }
        }

        public static IMembershipEncoder GetEncoder(this IRequestContext context, string protocolType = null)
        {
            var type = protocolType ?? context.ProtocolType;
            IMembershipEncoder encoder;
            if (type != null && context.Encoders != null && context.Encoders.TryGetValue(type, out encoder) && encoder != null) return encoder;

            throw new ArgumentOutOfRangeException(nameof(protocolType), $"Unknown protocol type {protocolType}");
        }

        public static IKafkaWriter Write(this IKafkaWriter writer, IMemberMetadata metadata, IMembershipEncoder encoder)
        {
            encoder.EncodeMetadata(writer, metadata);
            return writer;
        }

        public static IKafkaWriter Write(this IKafkaWriter writer, IMemberAssignment assignment, IMembershipEncoder encoder)
        {
            encoder.EncodeAssignment(writer, assignment);
            return writer;
        }

        public static IKafkaWriter Write(this IKafkaWriter writer, IImmutableList<int> values)
        {
            writer.Write(values.Count);
            foreach (var value in values) {
                writer.Write(value);
            }
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
