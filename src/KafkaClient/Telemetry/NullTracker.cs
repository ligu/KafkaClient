using System;
using KafkaClient.Connections;

namespace KafkaClient.Telemetry
{
    public class NullTracker
    {
        public void Disconnected(Endpoint endpoint, Exception exception)
        {
        }

        public void Connecting(Endpoint endpoint, int attempt, TimeSpan elapsed)
        {
        }

        public void Connected(Endpoint endpoint, int attempt, TimeSpan elapsed)
        {
        }

        public void WriteEnqueued(Endpoint endpoint, DataPayload payload)
        {
        }

        public void Writing(Endpoint endpoint, DataPayload payload)
        {
        }

        public void Written(Endpoint endpoint, DataPayload payload, TimeSpan elapsed)
        {
        }

        public void WriteFailed(Endpoint endpoint, DataPayload payload, TimeSpan elapsed, Exception exception)
        {
        }

        public void Reading(Endpoint endpoint, int size)
        {
        }

        public void ReadingChunk(Endpoint endpoint, int size, int read, TimeSpan elapsed)
        {
        }

        public void ReadChunk(Endpoint endpoint, int size, int remaining, int read, TimeSpan elapsed)
        {
        }

        public void Read(Endpoint endpoint, byte[] buffer, TimeSpan elapsed)
        {
        }

        public void ReadFailed(Endpoint endpoint, int size, TimeSpan elapsed, Exception exception)
        {
        }

        public void ProduceRequestMessages(int messages, int requestBytes, int compressionDiffBytes)
        {
        }
    }
}