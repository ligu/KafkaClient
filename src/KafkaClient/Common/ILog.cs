using System;
using System.Runtime.CompilerServices;

namespace KafkaClient.Common
{
    public struct LogEvent
    {
        public static LogEvent Create(
            string message, 
            [CallerFilePath] string sourceFile = null, 
            [CallerLineNumber] int sourceLine = 0)
        {
            return new LogEvent(message, null, sourceFile, sourceLine > 0 ? sourceLine : (int?)null);
        }

        public static LogEvent Create(
            Exception exception,
            string message = null, 
            [CallerFilePath] string sourceFile = null, 
            [CallerLineNumber] int sourceLine = 0)
        {
            return new LogEvent(message, exception, sourceFile, sourceLine > 0 ? sourceLine : (int?)null);
        }

        public LogEvent(string message, Exception exception, string sourceFile, int? sourceLine)
        {
            Message = message;
            Exception = exception;
            SourceFile = sourceFile;
            SourceLine = sourceLine;
        }

        public string Message { get; }
        public Exception Exception { get; }
        public string SourceFile { get; }
        public int? SourceLine { get; }
    }

    public static class LogExtensions
    {
        /// <summary>
        /// Record debug information using the String.Format syntax.
        /// </summary>
        public static void DebugFormat(this ILog log, string format, params object[] args)
        {
            log.Write(LogLevel.Debug, () => LogEvent.Create(string.Format(format, args)));
        }

        /// <summary>
        /// Record debug information using the String.Format syntax.
        /// </summary>
        public static void DebugFormat(this ILog log, Exception exception, string format = null, params object[] args)
        {
            log.Write(LogLevel.Debug, () => LogEvent.Create(exception, string.Format(format, args)));
        }

        /// <summary>
        /// Record info information using the String.Format syntax.
        /// </summary>
        public static void InfoFormat(this ILog log, string format, params object[] args)
        {
            log.Write(LogLevel.Info, () => LogEvent.Create(string.Format(format, args)));
        }

        /// <summary>
        /// Record info information using the String.Format syntax.
        /// </summary>
        public static void InfoFormat(this ILog log, Exception exception, string format = null, params object[] args)
        {
            log.Write(LogLevel.Info, () => LogEvent.Create(exception, string.Format(format, args)));
        }

        /// <summary>
        /// Record warning information using the String.Format syntax.
        /// </summary>
        public static void WarnFormat(this ILog log, string format, params object[] args)
        {
            log.Write(LogLevel.Warn, () => LogEvent.Create(string.Format(format, args)));
        }

        /// <summary>
        /// Record warning information using the String.Format syntax.
        /// </summary>
        public static void WarnFormat(this ILog log, Exception exception, string format = null, params object[] args)
        {
            log.Write(LogLevel.Warn, () => LogEvent.Create(exception, string.Format(format, args)));
        }

        /// <summary>
        /// Record debug information to the log.
        /// </summary>
        public static void Debug(this ILog log, Func<LogEvent> producer)
        {
            log.Write(LogLevel.Debug, producer);
        }

        /// <summary>
        /// Record information to the log.
        /// </summary>
        public static void Info(this ILog log, Func<LogEvent> producer)
        {
            log.Write(LogLevel.Info, producer);
        }

        /// <summary>
        /// Record warning information to the log.
        /// </summary>
        public static void Warn(this ILog log, Func<LogEvent> producer)
        {
            log.Write(LogLevel.Warn, producer);
        }

        /// <summary>
        /// Record error information to the log.
        /// </summary>
        public static void Error(this ILog log, LogEvent logEvent)
        {
            log.Write(LogLevel.Error, () => logEvent);
        }
    }

    public interface ILog
    {
        /// <summary>
        /// Record logging details lazily to avoid unnecessary computation costs
        /// </summary>
        void Write(LogLevel level, Func<LogEvent> producer);
    }
}