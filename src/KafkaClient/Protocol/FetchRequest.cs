using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    public class FetchRequest : KafkaRequest, IKafkaRequest<FetchResponse>
    {
        public FetchRequest(Fetch fetch, TimeSpan? maxWaitTime = null, int minBytes = DefaultMinBlockingByteBufferSize) 
            : this (new []{ fetch }, maxWaitTime, minBytes)
        {
        }

        public FetchRequest(IEnumerable<Fetch> fetches = null, TimeSpan? maxWaitTime = null, int minBytes = DefaultMinBlockingByteBufferSize) 
            : base(ApiKeyRequestType.Fetch)
        {
            MaxWaitTime = maxWaitTime.GetValueOrDefault(TimeSpan.FromMilliseconds(DefaultMaxBlockingWaitTime));
            MinBytes = minBytes;
            Fetches = ImmutableList<Fetch>.Empty.AddNotNullRange(fetches);
        }

        internal const int DefaultMinBlockingByteBufferSize = 4096;
        internal const int DefaultBufferSize = DefaultMinBlockingByteBufferSize * 8;
        internal const int DefaultMaxBlockingWaitTime = 5000;

        /// <summary>
        /// The max wait time is the maximum amount of time to block waiting if insufficient data is available at the time the request is issued.
        /// </summary>
        public TimeSpan MaxWaitTime { get; }

        /// <summary>
        /// This is the minimum number of bytes of messages that must be available to give a response.
        /// If the client sets this to 0 the server will always respond immediately, however if there is no new data since their last request they will just get back empty message sets.
        /// If this is set to 1, the server will respond as soon as at least one partition has at least 1 byte of data or the specified timeout occurs.
        /// By setting higher values in combination with the timeout the consumer can tune for throughput and trade a little additional latency for reading only large chunks of data
        /// (e.g. setting MaxWaitTime to 100 ms and setting MinBytes to 64k would allow the server to wait up to 100ms to try to accumulate 64k of data before responding).
        /// </summary>
        public int MinBytes { get; }

        public ImmutableList<Fetch> Fetches { get; }
    }
}