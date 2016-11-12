using System;
using System.Collections.Concurrent;
using System.Threading;
using KafkaClient.Protocol;

namespace KafkaClient.Telemetry
{
    public class ApiStatistics : Statistics
    {
        public ApiStatistics(DateTime startedAt, TimeSpan duration)
            : base(startedAt, duration)
        {
            RequestsAttempted = new ConcurrentDictionary<ApiKeyRequestType, int>();
            Requests = new ConcurrentDictionary<ApiKeyRequestType, int>();
            RequestsFailed = new ConcurrentDictionary<ApiKeyRequestType, int>();
        }

        public ConcurrentDictionary<ApiKeyRequestType, int> RequestsAttempted { get; }

        public void Attempt(ApiKeyRequestType type)
        {
            RequestsAttempted.AddOrUpdate(type, 1, (key, old) => old + 1);
        }

        public ConcurrentDictionary<ApiKeyRequestType, int> Requests { get; }

        private long _duration = 0L;
        public TimeSpan Duration => TimeSpan.FromTicks(_duration);

        public void Success(ApiKeyRequestType type, TimeSpan duration)
        {
            Requests.AddOrUpdate(type, 1, (key, old) => old + 1);
            Interlocked.Add(ref _duration, duration.Ticks);
        }

        public ConcurrentDictionary<ApiKeyRequestType, int> RequestsFailed { get; }

        public void Failure(ApiKeyRequestType type, TimeSpan duration)
        {
            RequestsFailed.AddOrUpdate(type, 1, (key, old) => old + 1);
            Interlocked.Add(ref _duration, duration.Ticks);
        }

        private int _messagesProduced = 0;
        public int MessagesProduced => _messagesProduced;
        private int _bytesProduced = 0;
        public int BytesProduced => _bytesProduced;
        private int _bytesCompressed = 0;
        public int BytesCompressed => _bytesCompressed;

        public void Produce(int messages, int bytes, int bytesCompressedAway)
        {
            Interlocked.Add(ref _messagesProduced, messages);
            Interlocked.Add(ref _bytesProduced, bytes);
            Interlocked.Add(ref _bytesCompressed, bytesCompressedAway);
        }
    }
}