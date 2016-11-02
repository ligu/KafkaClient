using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using KafkaClient.Common;
using KafkaClient.Connections;

namespace KafkaClient
{
    /// <summary>
    /// Statistics tracker uses circular buffers to capture a maximum set of current statistics.
    /// </summary>
    //TODO: remove static StatisticsTracker and create one for each tcp socket
    //TODO:all it's operation need to be on different thread pool with one thread
    [Obsolete("This is here for reference more than anything else -- hooks are available to make it work, but it needs a rewrite")]

    public static class StatisticsTracker
    {
        public static event Action<StatisticsSummary> OnStatisticsHeartbeat;

        // ReSharper disable once NotAccessedField.Local
        private static readonly IScheduledTimer HeartbeatTimer;
        private static readonly Gauges Gauges = new Gauges();
        private static readonly ConcurrentCircularBuffer<ProduceRequestStatistic> ProduceRequestStatistics = new ConcurrentCircularBuffer<ProduceRequestStatistic>(500);
        private static readonly ConcurrentCircularBuffer<NetworkWriteStatistic> CompletedNetworkWriteStatistics = new ConcurrentCircularBuffer<NetworkWriteStatistic>(500);
        private static readonly ConcurrentDictionary<int, NetworkWriteStatistic> NetworkWriteQueuedIndex = new ConcurrentDictionary<int, NetworkWriteStatistic>();

        static StatisticsTracker()
        {
            HeartbeatTimer = new ScheduledTimer()
                .StartingAt(DateTime.Now)
                .Every(TimeSpan.FromSeconds(5))
                .Do(HeartBeatAction)
                .Begin();
        }

        private static void HeartBeatAction()
        {
            OnStatisticsHeartbeat?.Invoke(
                new StatisticsSummary(ProduceRequestStatistics.ToList(),
                    NetworkWriteQueuedIndex.Values.ToList(),
                    CompletedNetworkWriteStatistics.ToList(),
                    Gauges));
        }

        public static void RecordProduceRequest(int messageCount, int payloadBytes, int compressedBytes)
        {
            ProduceRequestStatistics.Enqueue(new ProduceRequestStatistic(messageCount, payloadBytes, compressedBytes));
        }

        public static IDisposable Gauge(StatisticGauge type)
        {
            switch (type)
            {
                case StatisticGauge.ActiveReadOperation:
                    Interlocked.Increment(ref Gauges.ActiveReadOperation);
                    return new Disposable(() => Interlocked.Decrement(ref Gauges.ActiveReadOperation));

                case StatisticGauge.ActiveWriteOperation:
                    Interlocked.Increment(ref Gauges.ActiveWriteOperation);
                    return new Disposable(() => Interlocked.Decrement(ref Gauges.ActiveWriteOperation));

                case StatisticGauge.QueuedWriteOperation:
                    Interlocked.Increment(ref Gauges.QueuedWriteOperation);
                    return new Disposable(() => Interlocked.Decrement(ref Gauges.QueuedWriteOperation));

                default:
                    return Disposable.None;
            }
        }

        internal static IDisposable TrackNetworkWrite(SocketPayloadWriteTask writeTask)
        {
            var sw = Stopwatch.StartNew();
            Interlocked.Increment(ref Gauges.ActiveWriteOperation);
            return new Disposable(
                () => {
                    Interlocked.Decrement(ref Gauges.ActiveWriteOperation);
                    CompleteNetworkWrite(writeTask.Payload, sw.ElapsedMilliseconds, writeTask.Tcs.Task.Exception != null);
                });
        }

        public static void QueueNetworkWrite(Endpoint endpoint, DataPayload payload)
        {
            if (payload.TrackPayload == false) return;
            if (payload.Buffer.Length > 64) return;

            var stat = new NetworkWriteStatistic(endpoint, payload);
            NetworkWriteQueuedIndex.TryAdd(payload.CorrelationId, stat);
            Interlocked.Increment(ref Gauges.QueuedWriteOperation);
        }

        public static void CompleteNetworkWrite(DataPayload payload, long milliseconds, bool failed)
        {
            if (payload.TrackPayload == false) return;
            if (payload.Buffer.Length > 64) return;

            NetworkWriteStatistic stat;
            if (NetworkWriteQueuedIndex.TryRemove(payload.CorrelationId, out stat))
            {
                stat.SetCompleted(milliseconds, failed);
                CompletedNetworkWriteStatistics.Enqueue(stat);
            }
            Interlocked.Decrement(ref Gauges.QueuedWriteOperation);
        }

        /// <summary>
        /// Use of this class outside the StatisticsTracker has unexpected behavior
        /// </summary>
        public class ConcurrentCircularBuffer<T> : IEnumerable<T>
        {
            private readonly int _maxSize;

            private long _head = -1;
            private readonly T[] _values;

            public ConcurrentCircularBuffer(int max)
            {
                _maxSize = max;
                _values = new T[_maxSize];
            }

            public int MaxSize => _maxSize;

            public long Count
            {
                get
                {
                    long head = Interlocked.Read(ref _head);
                    if (head == -1) return 0;
                    if (head >= MaxSize) return MaxSize;
                    return head + 1;
                }
            }

            public ConcurrentCircularBuffer<T> Enqueue(T obj)
            {
                //if more then MaxSize thread will do Enqueue the order in not guaranteed and with object may erase each other
                var currentHead = Interlocked.Increment(ref _head);
                long index = currentHead % MaxSize;
                _values[index] = obj;
                return this;
            }

            public IEnumerator<T> GetEnumerator()
            {
                long head = Interlocked.Read(ref _head);
                for (int i = 0; i < Count; i++) {
                    yield return _values[head % MaxSize + i];
                }
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }
        }
    }

    public enum StatisticGauge
    {
        QueuedWriteOperation,
        ActiveWriteOperation,
        ActiveReadOperation
    }

    public class StatisticsSummary
    {
        public ProduceRequestSummary ProduceRequestSummary { get; private set; }
        public List<NetworkWriteSummary> NetworkWriteSummaries { get; }

        public List<ProduceRequestStatistic> ProduceRequestStatistics { get; }
        public List<NetworkWriteStatistic> CompletedNetworkWriteStatistics { get; private set; }
        public List<NetworkWriteStatistic> QueuedNetworkWriteStatistics { get; private set; }
        public Gauges Gauges { get; }

        public StatisticsSummary(List<ProduceRequestStatistic> produceRequestStatistics,
                                 List<NetworkWriteStatistic> queuedWrites,
                                 List<NetworkWriteStatistic> completedWrites,
                                 Gauges gauges)
        {
            ProduceRequestStatistics = produceRequestStatistics;
            QueuedNetworkWriteStatistics = queuedWrites;
            CompletedNetworkWriteStatistics = completedWrites;
            Gauges = gauges;

            if (queuedWrites.Count > 0 || completedWrites.Count > 0)
            {
                var queuedSummary = queuedWrites.GroupBy(x => x.Endpoint)
                    .Select(e => new
                    {
                        Endpoint = e.Key,
                        QueuedSummary = new NetworkQueueSummary
                        {
                            SampleSize = e.Count(),
                            OldestBatchInQueue = e.Max(x => x.TotalDuration),
                            BytesQueued = e.Sum(x => x.Payload.Buffer.Length),
                            QueuedMessages = e.Sum(x => x.Payload.MessageCount),
                            QueuedBatchCount = Gauges.QueuedWriteOperation,
                        }
                    }).ToList();

                var networkWriteSampleTimespan = completedWrites.Count <= 0 ? TimeSpan.FromMilliseconds(0) : DateTime.UtcNow - completedWrites.Min(x => x.CreatedOnUtc);
                var completedSummary = completedWrites.GroupBy(x => x.Endpoint)
                    .Select(e =>
                        new
                        {
                            Endpoint = e.Key,
                            CompletedSummary = new NetworkTcpSummary
                            {
                                MessagesPerSecond = (int)(e.Sum(x => x.Payload.MessageCount) /
                                                 networkWriteSampleTimespan.TotalSeconds),
                                MessagesLastBatch = e.OrderByDescending(x => x.CompletedOnUtc).Select(x => x.Payload.MessageCount).FirstOrDefault(),
                                MaxMessagesPerSecond = e.Max(x => x.Payload.MessageCount),
                                BytesPerSecond = (int)(e.Sum(x => x.Payload.Buffer.Length) /
                                                 networkWriteSampleTimespan.TotalSeconds),
                                AverageWriteDuration = TimeSpan.FromMilliseconds(e.Sum(x => x.WriteDuration.TotalMilliseconds) /
                                                       completedWrites.Count),
                                AverageTotalDuration = TimeSpan.FromMilliseconds(e.Sum(x => x.TotalDuration.TotalMilliseconds) /
                                                       completedWrites.Count),
                                SampleSize = completedWrites.Count
                            }
                        }
                    ).ToList();

                NetworkWriteSummaries = new List<NetworkWriteSummary>();
                var endpoints = queuedSummary.Select(x => x.Endpoint).Union(completedWrites.Select(x => x.Endpoint));
                foreach (var endpoint in endpoints)
                {
                    NetworkWriteSummaries.Add(new NetworkWriteSummary
                    {
                        Endpoint = endpoint,
                        QueueSummary = queuedSummary.Where(x => x.Endpoint.Equals(endpoint)).Select(x => x.QueuedSummary).FirstOrDefault(),
                        TcpSummary = completedSummary.Where(x => x.Endpoint.Equals(endpoint)).Select(x => x.CompletedSummary).FirstOrDefault()
                    });
                }
            }
            else
            {
                NetworkWriteSummaries = new List<NetworkWriteSummary>();
            }

            if (ProduceRequestStatistics.Count > 0)
            {
                var produceRequestSampleTimespan = DateTime.UtcNow -
                                                   ProduceRequestStatistics.Min(x => x.CreatedOnUtc);

                ProduceRequestSummary = new ProduceRequestSummary
                {
                    SampleSize = ProduceRequestStatistics.Count,
                    MessageCount = ProduceRequestStatistics.Sum(s => s.MessageCount),
                    MessageBytesPerSecond = (int)
                            (ProduceRequestStatistics.Sum(s => s.MessageBytes) / produceRequestSampleTimespan.TotalSeconds),
                    PayloadBytesPerSecond = (int)
                            (ProduceRequestStatistics.Sum(s => s.PayloadBytes) / produceRequestSampleTimespan.TotalSeconds),
                    CompressedBytesPerSecond = (int)
                            (ProduceRequestStatistics.Sum(s => s.CompressedBytes) / produceRequestSampleTimespan.TotalSeconds),
                    AverageCompressionRatio =
                        Math.Round(ProduceRequestStatistics.Sum(s => s.CompressionRatio) / ProduceRequestStatistics.Count, 4),
                    MessagesPerSecond = (int)
                            (ProduceRequestStatistics.Sum(x => x.MessageCount) / produceRequestSampleTimespan.TotalSeconds)
                };
            }
            else
            {
                ProduceRequestSummary = new ProduceRequestSummary();
            }
        }
    }

    public class Gauges
    {
        public int ActiveWriteOperation;
        public int ActiveReadOperation;
        public int QueuedWriteOperation;
    }

    public class NetworkWriteStatistic
    {
        public DateTime CreatedOnUtc { get; }
        public DateTime CompletedOnUtc { get; private set; }
        public bool IsCompleted { get; private set; }
        public bool IsFailed { get; private set; }
        public Endpoint Endpoint { get; }
        public DataPayload Payload { get; }
        public TimeSpan TotalDuration => (IsCompleted ? CompletedOnUtc : DateTime.UtcNow) - CreatedOnUtc;
        public TimeSpan WriteDuration { get; private set; }

        public NetworkWriteStatistic(Endpoint endpoint, DataPayload payload)
        {
            CreatedOnUtc = DateTime.UtcNow;
            Endpoint = endpoint;
            Payload = payload;
        }

        public void SetCompleted(long milliseconds, bool failedFlag)
        {
            IsCompleted = true;
            IsFailed = failedFlag;
            CompletedOnUtc = DateTime.UtcNow;
            WriteDuration = TimeSpan.FromMilliseconds(milliseconds);
        }

        public void SetSuccess(bool failed)
        {
            IsFailed = failed;
        }
    }

    public class NetworkWriteSummary
    {
        public Endpoint Endpoint;

        public NetworkTcpSummary TcpSummary = new NetworkTcpSummary();
        public NetworkQueueSummary QueueSummary = new NetworkQueueSummary();
    }

    public class NetworkQueueSummary
    {
        public int BytesQueued;
        public double KilobytesQueued => MathHelper.ConvertToKilobytes(BytesQueued);
        public TimeSpan OldestBatchInQueue { get; set; }
        public int QueuedMessages { get; set; }
        public int QueuedBatchCount;
        public int SampleSize { get; set; }
    }

    public class NetworkTcpSummary
    {
        public int MessagesPerSecond;
        public int MaxMessagesPerSecond;
        public int BytesPerSecond;
        public TimeSpan AverageWriteDuration;
        public double KilobytesPerSecond => MathHelper.ConvertToKilobytes(BytesPerSecond);
        public TimeSpan AverageTotalDuration { get; set; }
        public int SampleSize { get; set; }
        public int MessagesLastBatch { get; set; }
    }

    public class ProduceRequestSummary
    {
        public int SampleSize;
        public int MessageCount;
        public int MessagesPerSecond;
        public int MessageBytesPerSecond;
        public double MessageKilobytesPerSecond => MathHelper.ConvertToKilobytes(MessageBytesPerSecond);
        public int PayloadBytesPerSecond;
        public double PayloadKilobytesPerSecond => MathHelper.ConvertToKilobytes(PayloadBytesPerSecond);
        public int CompressedBytesPerSecond;
        public double CompressedKilobytesPerSecond => MathHelper.ConvertToKilobytes(CompressedBytesPerSecond);
        public double AverageCompressionRatio;
    }

    public class ProduceRequestStatistic
    {
        public DateTime CreatedOnUtc { get; }
        public int MessageCount { get; }
        public int MessageBytes { get; }
        public int PayloadBytes { get; }
        public int CompressedBytes { get; }
        public double CompressionRatio { get; }

        public ProduceRequestStatistic(int messageCount, int payloadBytes, int compressedBytes)
        {
            CreatedOnUtc = DateTime.UtcNow;
            MessageCount = messageCount;
            MessageBytes = payloadBytes + compressedBytes;
            PayloadBytes = payloadBytes;
            CompressedBytes = compressedBytes;

            CompressionRatio = MessageBytes == 0 ? 0 : Math.Round((double)compressedBytes / MessageBytes, 4);
        }
    }

    public static class MathHelper
    {
        public static double ConvertToMegabytes(int bytes)
        {
            if (bytes == 0) return 0;
            return Math.Round((double)bytes / 1048576, 4);
        }

        public static double ConvertToKilobytes(int bytes)
        {
            if (bytes == 0) return 0;
            return Math.Round((double)bytes / 1000, 4);
        }
    }
}