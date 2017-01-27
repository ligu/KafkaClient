using System;
using System.Collections.Concurrent;
using System.Threading;
using KafkaClient.Protocol;

namespace KafkaClient.Telemetry
{
    public class ApiStatistics : Statistics
    {
        public ApiStatistics(DateTimeOffset startedAt, TimeSpan duration)
            : base(startedAt, duration)
        {
            RequestsAttempted = new ConcurrentDictionary<ApiKey, int>();
            Requests = new ConcurrentDictionary<ApiKey, int>();
            RequestsFailed = new ConcurrentDictionary<ApiKey, int>();
        }

        public ConcurrentDictionary<ApiKey, int> RequestsAttempted { get; }

        public void Attempt(ApiKey apiKey)
        {
            RequestsAttempted.AddOrUpdate(apiKey, 1, (key, old) => old + 1);
        }

        public ConcurrentDictionary<ApiKey, int> Requests { get; }

        private long _duration;
        public TimeSpan Duration => TimeSpan.FromTicks(_duration);

        public void Success(ApiKey apiKey, TimeSpan duration)
        {
            Requests.AddOrUpdate(apiKey, 1, (key, old) => old + 1);
            Interlocked.Add(ref _duration, duration.Ticks);
        }

        public ConcurrentDictionary<ApiKey, int> RequestsFailed { get; }

        public void Failure(ApiKey apiKey, TimeSpan duration)
        {
            RequestsFailed.AddOrUpdate(apiKey, 1, (key, old) => old + 1);
            Interlocked.Add(ref _duration, duration.Ticks);
        }

        private int _messages;
        public int Messages => _messages;
        private int _messageBytes;
        public int MessageBytes => _messageBytes;
        private int _messageTcpBytes;
        public int MessageTcpBytes => _messageTcpBytes;

        public void Produce(int messages, int wireBytes, int bytesCompressed)
        {
            Interlocked.Add(ref _messages, messages);
            Interlocked.Add(ref _messageTcpBytes, wireBytes);
            Interlocked.Add(ref _messageBytes, wireBytes + bytesCompressed);
        }
    }
}