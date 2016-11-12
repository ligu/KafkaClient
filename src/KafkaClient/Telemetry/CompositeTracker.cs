using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Connections;

namespace KafkaClient.Telemetry
{
    public class CompositeTracker : ITrackEvents
    {
        private readonly IImmutableList<ITrackEvents> _trackers;

        public CompositeTracker(IEnumerable<ITrackEvents> trackers)
        {
            _trackers = ImmutableList<ITrackEvents>.Empty.AddRange(trackers);
        }

        public void Disconnected(Endpoint endpoint, Exception exception)
        {
            foreach (var tracker in _trackers) {
                tracker.Disconnected(endpoint, exception);
            }
        }

        public void Connecting(Endpoint endpoint, int attempt, TimeSpan elapsed)
        {
            foreach (var tracker in _trackers) {
                tracker.Connecting(endpoint, attempt, elapsed);
            }
        }

        public void Connected(Endpoint endpoint, int attempt, TimeSpan elapsed)
        {
            foreach (var tracker in _trackers) {
                tracker.Connected(endpoint, attempt, elapsed);
            }
        }

        public void WriteEnqueued(Endpoint endpoint, DataPayload payload)
        {
            foreach (var tracker in _trackers) {
                tracker.WriteEnqueued(endpoint, payload);
            }
        }

        public void Writing(Endpoint endpoint, DataPayload payload)
        {
            foreach (var tracker in _trackers) {
                tracker.Writing(endpoint, payload);
            }
        }

        public void Written(Endpoint endpoint, DataPayload payload, TimeSpan elapsed)
        {
            foreach (var tracker in _trackers) {
                tracker.Written(endpoint, payload, elapsed);
            }
        }

        public void WriteFailed(Endpoint endpoint, DataPayload payload, TimeSpan elapsed, Exception exception)
        {
            foreach (var tracker in _trackers) {
                tracker.WriteFailed(endpoint, payload, elapsed, exception);
            }
        }

        public void Reading(Endpoint endpoint, int size)
        {
            foreach (var tracker in _trackers) {
                tracker.Reading(endpoint, size);
            }
        }

        public void ReadingChunk(Endpoint endpoint, int size, int read, TimeSpan elapsed)
        {
            foreach (var tracker in _trackers) {
                tracker.ReadingChunk(endpoint, size, read, elapsed);
            }
        }

        public void ReadChunk(Endpoint endpoint, int size, int remaining, int read, TimeSpan elapsed)
        {
            foreach (var tracker in _trackers) {
                tracker.ReadChunk(endpoint, size, remaining, read, elapsed);
            }
        }

        public void Read(Endpoint endpoint, byte[] buffer, TimeSpan elapsed)
        {
            foreach (var tracker in _trackers) {
                tracker.Read(endpoint, buffer, elapsed);
            }
        }

        public void ReadFailed(Endpoint endpoint, int size, TimeSpan elapsed, Exception exception)
        {
            foreach (var tracker in _trackers) {
                tracker.ReadFailed(endpoint, size, elapsed, exception);
            }
        }

        public void ProduceRequestMessages(int messages, int requestBytes, int compressionDiffBytes)
        {
            foreach (var tracker in _trackers) {
                tracker.ProduceRequestMessages(messages, requestBytes, compressionDiffBytes);
            }
        }
    }
}