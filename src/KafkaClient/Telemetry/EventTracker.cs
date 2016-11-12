using System;
using System.Collections.Concurrent;
using KafkaClient.Connections;

namespace KafkaClient.Telemetry
{
    public class EventTracker : ITrackEvents
    {
        private readonly int _maxSize;

        public EventTracker(int maxSize = 0)
        {
            _maxSize = maxSize;
        }

        private void Enqueue<T>(ConcurrentQueue<T> queue, T item)
        {
            queue.Enqueue(item);
            if (_maxSize <= 0 || queue.Count <= _maxSize) return;

            queue.TryDequeue(out item);
        }

        private void Enqueue<T1, T2>(ConcurrentQueue<Tuple<DateTime, T1, T2>> queue, T1 arg1, T2 arg2)
        {
            Enqueue(queue, new Tuple<DateTime, T1, T2>(DateTime.UtcNow, arg1, arg2));
        }

        private void Enqueue<T1, T2, T3>(ConcurrentQueue<Tuple<DateTime, T1, T2, T3>> queue, T1 arg1, T2 arg2, T3 arg3)
        {
            Enqueue(queue, new Tuple<DateTime, T1, T2, T3>(DateTime.UtcNow, arg1, arg2, arg3));
        }

        private void Enqueue<T1, T2, T3, T4>(ConcurrentQueue<Tuple<DateTime, T1, T2, T3, T4>> queue, T1 arg1, T2 arg2, T3 arg3, T4 arg4)
        {
            Enqueue(queue, new Tuple<DateTime, T1, T2, T3, T4>(DateTime.UtcNow, arg1, arg2, arg3, arg4));
        }
        private void Enqueue<T1, T2, T3, T4, T5>(ConcurrentQueue<Tuple<DateTime, T1, T2, T3, T4, T5>> queue, T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5)
        {
            Enqueue(queue, new Tuple<DateTime, T1, T2, T3, T4, T5>(DateTime.UtcNow, arg1, arg2, arg3, arg4, arg5));
        }

        public ConcurrentQueue<Tuple<DateTime, Endpoint, Exception>> DisconnectedEvents { get; } = new ConcurrentQueue<Tuple<DateTime, Endpoint, Exception>>();
        public void Disconnected(Endpoint endpoint, Exception exception)
        {
            Enqueue(DisconnectedEvents, endpoint, exception);
        }

        public ConcurrentQueue<Tuple<DateTime, Endpoint, int, TimeSpan>> ConnectingEvents { get; } = new ConcurrentQueue<Tuple<DateTime, Endpoint, int, TimeSpan>>();
        public void Connecting(Endpoint endpoint, int attempt, TimeSpan elapsed)
        {
            Enqueue(ConnectingEvents, endpoint, attempt, elapsed);
        }

        public ConcurrentQueue<Tuple<DateTime, Endpoint, int, TimeSpan>> ConnectedEvents { get; } = new ConcurrentQueue<Tuple<DateTime, Endpoint, int, TimeSpan>>();
        public void Connected(Endpoint endpoint, int attempt, TimeSpan elapsed)
        {
            Enqueue(ConnectedEvents, endpoint, attempt, elapsed);
        }

        public ConcurrentQueue<Tuple<DateTime, Endpoint, DataPayload>> WriteEnqueuedEvents { get; } = new ConcurrentQueue<Tuple<DateTime, Endpoint, DataPayload>>();
        public void WriteEnqueued(Endpoint endpoint, DataPayload payload)
        {
            Enqueue(WriteEnqueuedEvents, endpoint, payload);
        }

        public ConcurrentQueue<Tuple<DateTime, Endpoint, DataPayload>> WritingEvents { get; } = new ConcurrentQueue<Tuple<DateTime, Endpoint, DataPayload>>();
        public void Writing(Endpoint endpoint, DataPayload payload)
        {
            Enqueue(WritingEvents, endpoint, payload);
        }

        public ConcurrentQueue<Tuple<DateTime, Endpoint, DataPayload, TimeSpan>> WrittenEvents { get; } = new ConcurrentQueue<Tuple<DateTime, Endpoint, DataPayload, TimeSpan>>();
        public void Written(Endpoint endpoint, DataPayload payload, TimeSpan elapsed)
        {
            Enqueue(WrittenEvents, endpoint, payload, elapsed);
        }

        public ConcurrentQueue<Tuple<DateTime, Endpoint, DataPayload, TimeSpan, Exception>> WriteFailedEvents { get; } = new ConcurrentQueue<Tuple<DateTime, Endpoint, DataPayload, TimeSpan, Exception>>();
        public void WriteFailed(Endpoint endpoint, DataPayload payload, TimeSpan elapsed, Exception exception)
        {
            Enqueue(WriteFailedEvents, endpoint, payload, elapsed, exception);
        }

        public ConcurrentQueue<Tuple<DateTime, Endpoint, int>> ReadingEvents { get; } = new ConcurrentQueue<Tuple<DateTime, Endpoint, int>>();
        public void Reading(Endpoint endpoint, int size)
        {
            Enqueue(ReadingEvents, endpoint, size);
        }

        public ConcurrentQueue<Tuple<DateTime, Endpoint, int, int, TimeSpan>> ReadingChunkEvents { get; } = new ConcurrentQueue<Tuple<DateTime, Endpoint, int, int, TimeSpan>>();
        public void ReadingChunk(Endpoint endpoint, int size, int read, TimeSpan elapsed)
        {
            Enqueue(ReadingChunkEvents, endpoint, size, read, elapsed);
        }

        public ConcurrentQueue<Tuple<DateTime, Endpoint, int, int, int, TimeSpan>> ReadChunkEvents { get; } = new ConcurrentQueue<Tuple<DateTime, Endpoint, int, int, int, TimeSpan>>();
        public void ReadChunk(Endpoint endpoint, int size, int remaining, int read, TimeSpan elapsed)
        {
            Enqueue(ReadChunkEvents, endpoint, size, remaining, read, elapsed);
        }

        public ConcurrentQueue<Tuple<DateTime, Endpoint, byte[], TimeSpan>> ReadEvents { get; } = new ConcurrentQueue<Tuple<DateTime, Endpoint, byte[], TimeSpan>>();
        public void Read(Endpoint endpoint, byte[] buffer, TimeSpan elapsed)
        {
            Enqueue(ReadEvents, endpoint, buffer, elapsed);
        }

        public ConcurrentQueue<Tuple<DateTime, Endpoint, int, TimeSpan, Exception>> ReadFailedEvents { get; } = new ConcurrentQueue<Tuple<DateTime, Endpoint, int, TimeSpan, Exception>>();
        public void ReadFailed(Endpoint endpoint, int size, TimeSpan elapsed, Exception exception)
        {
            Enqueue(ReadFailedEvents, endpoint, size, elapsed, exception);
        }

        public ConcurrentQueue<Tuple<DateTime, int, int, int>> ProduceRequestMessageEvents { get; } = new ConcurrentQueue<Tuple<DateTime, int, int, int>>();
        public void ProduceRequestMessages(int messages, int requestBytes, int compressedBytes)
        {
            Enqueue(ProduceRequestMessageEvents, messages, requestBytes, compressedBytes);
        }
    }
}