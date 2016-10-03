using System;
using System.Collections.Generic;
using KafkaClient.Common;

namespace KafkaClient.Tests.Helpers
{
    public class MemoryLog : ILog
    {
        public IList<Tuple<LogLevel, LogEvent>> LogEvents { get; } = new List<Tuple<LogLevel, LogEvent>>();

        /// <inheritdoc />
        public void Write(LogLevel level, Func<LogEvent> producer)
        {
            LogEvents.Add(new Tuple<LogLevel, LogEvent>(level, producer()));
        }

        // for mocking interception purposes
        public virtual void Log(LogLevel level, LogEvent logEvent)
        {
        }
    }
}