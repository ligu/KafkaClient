using System;
using KafkaClient.Connections;
using KafkaClient.Protocol;

namespace KafkaClient.Telemetry
{
    public delegate void ProduceRequestMessages(int messages, int requestBytes, int compressionDiffBytes);
    public delegate void ConnectError(Endpoint endpoint, Exception exception);
    public delegate void Connecting(Endpoint endpoint, int attempt, TimeSpan elapsed);
    public delegate void Writing(Endpoint endpoint, ApiKeyRequestType type);
    public delegate void WriteSuccess(Endpoint endpoint, ApiKeyRequestType type, int bytesWritten, TimeSpan elapsed);
    public delegate void WriteError(Endpoint endpoint, ApiKeyRequestType type, TimeSpan elapsed, Exception exception);
    public delegate void Reading(Endpoint endpoint, int bytesAvailable);
    public delegate void ReadSuccess(Endpoint endpoint, int bytesRead, TimeSpan elapsed);
    public delegate void ReadError(Endpoint endpoint, int bytesAvailable, TimeSpan elapsed, Exception exception);
    public delegate void StartingBytes(Endpoint endpoint, int bytesAvailable);
    public delegate void FinishedBytes(Endpoint endpoint, int bytesAttempted, int bytesActual, TimeSpan elapsed);

    public interface IRaiseEvents
    {
        /// <summary>
        /// Triggered when the tcp socket is disconnected.
        /// </summary>
        ConnectError OnDisconnected { get; }

        /// <summary>
        /// Triggered when the tcp socket is connecting.
        /// </summary>
        Connecting OnConnecting { get; } 

        /// <summary>
        /// Triggered after the tcp socket is successfully connected.
        /// </summary>
        Connecting OnConnected { get; } 

        /// <summary>
        /// Triggered when writing to the tcp stream.
        /// </summary>
        Writing OnWriting { get; } 

        /// <summary>
        /// Triggered when writing a chunk of bytes to the tcp stream.
        /// </summary>
        StartingBytes OnWritingBytes { get; } 

        /// <summary>
        /// Triggered after successfully writing a chunk of bytes to the tcp stream.
        /// </summary>
        FinishedBytes OnWroteBytes { get; } 

        /// <summary>
        /// Triggered after having successfully written to the tcp stream.
        /// </summary>
        WriteSuccess OnWritten { get; } 

        /// <summary>
        /// Triggered after failing to write to the tcp stream.
        /// </summary>
        WriteError OnWriteFailed { get; } 

        /// <summary>
        /// Triggered when starting to read a message's bytes from the tcp stream.
        /// </summary>
        Reading OnReading { get; } 

        /// <summary>
        /// Triggered when reading a chunk of bytes from the tcp stream.
        /// </summary>
        StartingBytes OnReadingBytes { get; } 

        /// <summary>
        /// Triggered after successfully reading a chunk of bytes from the tcp stream.
        /// </summary>
        FinishedBytes OnReadBytes { get; } 

        /// <summary>
        /// Triggered after having successfully read a message's bytes from the tcp stream.
        /// </summary>
        ReadSuccess OnRead { get; } 

        /// <summary>
        /// Triggered after failing to read from the tcp stream.
        /// </summary>
        ReadError OnReadFailed { get; } 

        /// <summary>
        /// Triggered when encoding ProduceRequest messages.
        /// </summary>
        ProduceRequestMessages OnProduceRequestMessages { get; }        
    }
}