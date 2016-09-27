using System;
using KafkaClient.Protocol;

namespace KafkaClient
{
    public interface IProducerConfiguration
    {
        /// <summary>
        /// The maximum async calls allowed before blocking new requests. <value>-1</value> indicates unlimited.
        /// </summary>
        int RequestParallelization { get; }

        /// <summary>
        /// The number of messages to wait for before sending to kafka. The wait will last at most <see cref="BatchMaxDelay"/> before 
        /// sending what's already in the queue.
        /// </summary>
        int BatchSize { get; }

        /// <summary>
        /// The time to wait for a batch size of <see cref="BatchSize"/> before sending messages to kafka.
        /// </summary>
        TimeSpan BatchMaxDelay { get; }

        /// <summary>
        /// The default codec to apply when sending messages.
        /// </summary>
        MessageCodec Codec { get; }

        /// <summary>
        /// The required level of acknowlegment from the kafka server: 0=none, 1=writen to leader, 2+=writen to replicas, -1=writen to all replicas.
        /// </summary>
        short Acks { get; }

        /// <summary>
        /// The interal kafka timeout to wait for the requested level of ack to occur before returning.
        /// </summary>
        TimeSpan ServerAckTimeout { get; }

        /// <summary>
        /// The maximum time to wait for in-flight requests to complete, when stopping or disposing.
        /// </summary>
        TimeSpan StopTimeout { get; }
    }
}