using System;
using System.Diagnostics;

namespace KafkaClient.Common
{
    /// <summary>
    /// This class simply logs all information out to the Trace log provided by windows.
    /// The reason Trace is being used as the default it to remove extenal references from
    /// the base kafka-net package.  A proper logging framework like log4net is recommended.
    /// </summary>
    public class TraceLog : IKafkaLog
    {
        private readonly LogLevel _minLevel;

        public TraceLog(LogLevel minLevel)
        {
            _minLevel = minLevel;
        }

        public TraceLog()
        {
            _minLevel = LogLevel.Debug;
        }

        private void LogFormat(LogLevel level, Exception exception, string format, params object[] args)
        {
            if (level >= _minLevel) {
                var timestamp = DateTime.Now.ToString("hh:mm:ss-ffffff");
                var threadId = System.Threading.Thread.CurrentThread.ManagedThreadId;
                if (!string.IsNullOrEmpty(format)) {
                    var message = string.Format(format, args);
                    Trace.WriteLine(
                        exception != null
                            ? $"{timestamp} thread:[{threadId}] level:[{level}] Message:{message} Exception:{exception}"
                            : $"{timestamp} thread:[{threadId}] level:[{level}] Message:{message}");
                } else if (exception != null) {
                    Trace.WriteLine($"{timestamp} thread:[{threadId}] level:[{level}] Exception:{exception}");
                }
            }
        }

        public void DebugFormat(string format, params object[] args)
        {
            LogFormat(LogLevel.Debug, null, format, args);
        }

        public void DebugFormat(Exception exception, string format = null, params object[] args)
        {
            LogFormat(LogLevel.Debug, exception, format, args);
        }

        public void InfoFormat(string format, params object[] args)
        {
            LogFormat(LogLevel.Info, null, format, args);
        }

        public void InfoFormat(Exception exception, string format = null, params object[] args)
        {
            LogFormat(LogLevel.Info, exception, format, args);
        }

        public void WarnFormat(string format, params object[] args)
        {
            LogFormat(LogLevel.Warn, null, format, args);
        }

        public void WarnFormat(Exception exception, string format = null, params object[] args)
        {
            LogFormat(LogLevel.Warn, exception, format, args);
        }

        public void ErrorFormat(string format, params object[] args)
        {
            LogFormat(LogLevel.Error, null, format, args);
        }

        public void ErrorFormat(Exception exception, string format = null, params object[] args)
        {
            LogFormat(LogLevel.Error, exception, format, args);
        }
    }

    public enum LogLevel
    {
        Debug = 0,
        Info = 1,
        Warn = 2,
        Error = 3
    }
}