using System;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Tests
{
    public class ConsoleLog : ILog
    {
        private readonly LogLevel _minLevel;
        private readonly ImmutableDictionary<LogLevel, ConsoleColor> _colors;
        private readonly ImmutableDictionary<LogLevel, string> _levels;
        private static readonly object ConsoleWriterLock = new object();
        public ConsoleLog(LogLevel minLevel)
        {
            _minLevel = minLevel;
            _colors = ImmutableDictionary<LogLevel, ConsoleColor>.Empty
                .Add(LogLevel.Debug, ConsoleColor.DarkGray)
                .Add(LogLevel.Info, ConsoleColor.White)
                .Add(LogLevel.Warn, ConsoleColor.Yellow)
                .Add(LogLevel.Error, ConsoleColor.Red);
            _levels = ImmutableDictionary<LogLevel, string>
                .Empty
                .Add(LogLevel.Debug, "d")
                .Add(LogLevel.Info, "i")
                .Add(LogLevel.Warn, "w")
                .Add(LogLevel.Error, "e");
        }

        public ConsoleLog() : this (LogLevel.Debug)
        {
        }

        public void Write(LogLevel level, Func<LogEvent> producer)
        {
            if (level < _minLevel) return;

            var logEvent = producer();
            var text = ToText(level, logEvent);
            lock (ConsoleWriterLock) {
                Console.ForegroundColor = _colors[level];
                Console.WriteLine(text);
                Console.ResetColor();
            }
        }

        public string ToText(LogLevel level, LogEvent logEvent)
        {
            var timestamp = DateTime.Now.ToString("hh:mm:ss-ffffff");
            var threadId = System.Threading.Thread.CurrentThread.ManagedThreadId;
            var text = $"{timestamp} {_levels[level]}:{threadId,-3}";
            if (!string.IsNullOrEmpty(logEvent.Message)) {
                text += logEvent.Message;
            }
            if (logEvent.Exception != null) {
                text += $"\r\nException=\"{logEvent.Exception}\"";
            }
            text += $" in {logEvent.SourceFile}:line {logEvent.SourceLine.GetValueOrDefault()}";
            return text;
        }
    }
}