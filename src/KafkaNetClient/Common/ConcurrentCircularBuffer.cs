using System.Collections;
using System.Collections.Generic;
using System.Threading;

namespace KafkaNet.Common
{
    /// <summary>
    /// don't use this class out side the StatisticsTracker has unexpected behavior
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

        public int MaxSize { get { return _maxSize; } }

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
            for (int i = 0; i < Count; i++)
            {
                yield return _values[(head % MaxSize) + i];
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}