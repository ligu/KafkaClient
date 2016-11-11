using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using KafkaClient.Common;
using KafkaClient.Connections;

namespace KafkaClient.Telemetry
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
        //private static readonly IScheduledTimer HeartbeatTimer;
        private static readonly Gauges Gauges = new Gauges();
        private static readonly ConcurrentCircularBuffer<ProduceRequestStatistic> ProduceRequestStatistics = new ConcurrentCircularBuffer<ProduceRequestStatistic>(500);
        private static readonly ConcurrentCircularBuffer<NetworkWriteStatistic> CompletedNetworkWriteStatistics = new ConcurrentCircularBuffer<NetworkWriteStatistic>(500);
        private static readonly ConcurrentDictionary<int, NetworkWriteStatistic> NetworkWriteQueuedIndex = new ConcurrentDictionary<int, NetworkWriteStatistic>();

        static StatisticsTracker()
        {
            //HeartbeatTimer = new ScheduledTimer()
            //    .StartingAt(DateTime.Now)
            //    .Every(TimeSpan.FromSeconds(5))
            //    .Do(HeartBeatAction)
            //    .Begin();
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
}