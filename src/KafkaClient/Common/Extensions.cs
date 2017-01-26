using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Runtime.ExceptionServices;

namespace KafkaClient.Common
{
    public static class Extensions
    { 
        public static bool HasEqualElementsInOrder<T>(this IEnumerable<T> self, IEnumerable<T> other)
        {
            if (ReferenceEquals(self, other)) return true;
            if (ReferenceEquals(null, other)) return false;

            return self.Zip(other, (s, o) => Equals(s, o)).All(_ => _);
        }

        /// <summary>
        /// Attempts to prepare the exception for re-throwing by preserving the stack trace. The returned exception should be immediately thrown.
        /// </summary>
        /// <param name="exception">The exception. May not be <c>null</c>.</param>
        /// <returns>The <see cref="Exception"/> that was passed into this method.</returns>
        public static Exception PrepareForRethrow(this Exception exception)
        {
            ExceptionDispatchInfo.Capture(exception).Throw();

            // The code cannot ever get here. We just return a value to work around a badly-designed API (ExceptionDispatchInfo.Throw):
            //  https://connect.microsoft.com/VisualStudio/feedback/details/689516/exceptiondispatchinfo-api-modifications (http://www.webcitation.org/6XQ7RoJmO)
            return exception;
        }

        public static string ToStrings<T>(this IEnumerable<T> values)
        {
            return string.Join(",", values.Select(value => value.ToString()));
        }

        public static IImmutableList<T> AddNotNull<T>(this IImmutableList<T> list, T item) where T : class
        {
            return item != null ? list.Add(item) : list;
        }

        public static IImmutableList<T> AddNotNullRange<T>(this IImmutableList<T> list, IEnumerable<T> items)
        {
            return items != null ? list.AddRange(items) : list;
        }
        public static IImmutableDictionary<T, TValue> AddNotNullRange<T, TValue>(this IImmutableDictionary<T, TValue> dictionary, IEnumerable<KeyValuePair<T, TValue>> items)
        {
            return items != null ? dictionary.AddRange(items) : dictionary;
        }

        public static IKafkaWriter Write(this IKafkaWriter writer, IEnumerable<string> values, bool includeLength = false)
        {
            if (includeLength) {
                var valuesList = values.ToList();
                writer.Write(valuesList.Count);
                writer.Write(valuesList); // NOTE: !includeLength passed next time
                return writer;
            }

            foreach (var item in values) {
                writer.Write(item);
            }
            return writer;
        }


        public static Exception FlattenAggregates(this IEnumerable<Exception> exceptions)
        {
            var exceptionList = exceptions.ToArray();
            if (exceptionList.Length == 1) return exceptionList[0];

            return new AggregateException(exceptionList.SelectMany<Exception, Exception>(
                ex => {
                    var aggregateException = ex as AggregateException;
                    if (aggregateException != null) return aggregateException.InnerExceptions;
                    return new[] { ex };
                }));
        }
    }
}