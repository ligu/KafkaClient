using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    public class MetadataRequest : Request, IRequest<MetadataResponse>
    {
        public MetadataRequest(string topic)
            : this (new []{topic})
        {
        }

        public MetadataRequest(IEnumerable<string> topics = null) 
            : base(ApiKeyRequestType.Metadata)
        {
            Topics = ImmutableList<string>.Empty.AddNotNullRange(topics);
        }

        /// <summary>
        /// The list of topics to get metadata for.
        /// </summary>
        public ImmutableList<string> Topics { get; }
    }
}