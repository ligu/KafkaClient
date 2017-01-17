using System;
using KafkaClient.Connections;
using KafkaClient.Protocol;

namespace KafkaClient.Telemetry
{
    public interface ITrackEvents
    {
        void Disconnected(Endpoint endpoint, Exception exception);
        void Connecting(Endpoint endpoint, int attempt, TimeSpan elapsed);
        void Connected(Endpoint endpoint, int attempt, TimeSpan elapsed);
        void Writing(Endpoint endpoint, ApiKeyRequestType type);
        void WritingChunk(Endpoint endpoint, int bytesAvailable);
        void WroteChunk(Endpoint endpoint, int bytesAttempted, int bytesWritten, TimeSpan elapsed);
        void Written(Endpoint endpoint, ApiKeyRequestType type, int bytesWritten, TimeSpan elapsed);
        void WriteFailed(Endpoint endpoint, ApiKeyRequestType type, TimeSpan elapsed, Exception exception);
        void Reading(Endpoint endpoint, int bytesAvailable);
        void ReadingChunk(Endpoint endpoint, int bytesAvailable);
        void ReadChunk(Endpoint endpoint, int bytesAttempted, int bytesRead, TimeSpan elapsed);
        void Read(Endpoint endpoint, int bytesRead, TimeSpan elapsed);
        void ReadFailed(Endpoint endpoint, int bytesAvailable, TimeSpan elapsed, Exception exception);
        void ProduceRequestMessages(int messages, int requestBytes, int compressedBytes);
    }
}