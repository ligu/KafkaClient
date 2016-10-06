using System;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Tests.Helpers
{
    public class MemoryLog : ILog
    {
        public ImmutableList<Tuple<LogLevel, LogEvent>> LogEvents { get; private set; } = ImmutableList<Tuple<LogLevel, LogEvent>>.Empty;

        /// <inheritdoc />
        public void Write(LogLevel level, Func<LogEvent> producer)
        {
            LogEvents = LogEvents.Add(new Tuple<LogLevel, LogEvent>(level, producer()));
        }
    }
}