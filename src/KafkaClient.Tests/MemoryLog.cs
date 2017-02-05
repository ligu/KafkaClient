using System;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using KafkaClient.Common;

namespace KafkaClient.Tests
{
    public class MemoryLog : ILog
    {
        public ImmutableList<Tuple<LogLevel, LogEvent>> LogEvents { get; private set; } = ImmutableList<Tuple<LogLevel, LogEvent>>.Empty;

        public bool WriteToConsole { get; set; } = false;

        /// <inheritdoc />
        public void Write(LogLevel level, Func<LogEvent> producer)
        {
            var logEvent = producer();
            LogEvents = LogEvents.Add(new Tuple<LogLevel, LogEvent>(level, logEvent));
            if (WriteToConsole) {
                ConsoleLog.Write(level, ConsoleLog.ToText(level, logEvent));
            }
        }

        public string ToString(LogLevel level)
        {
            var buffer = new StringBuilder();
            foreach (var logEvent in LogEvents.Where(e => e.Item1 == level)) {
                buffer.AppendLine(ConsoleLog.ToText(logEvent.Item1, logEvent.Item2));
            }
            return buffer.ToString();
        }

        public override string ToString()
        {
            var buffer = new StringBuilder();
            foreach (var logEvent in LogEvents) {
                buffer.AppendLine(ConsoleLog.ToText(logEvent.Item1, logEvent.Item2));
            }
            return buffer.ToString();
        }
    }
}