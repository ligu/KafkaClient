using System.Collections.Generic;
using System.Collections.Immutable;

namespace KafkaNet.Protocol
{
    public class MetadataRequest : KafkaRequest, IKafkaRequest<MetadataResponse>
    {
        public MetadataRequest(string topic)
            : this (new []{topic})
        {
        }

        public MetadataRequest(IEnumerable<string> topics = null) 
            : base(ApiKeyRequestType.Metadata)
        {
            Topics = topics != null ? ImmutableList<string>.Empty.AddRange(topics) : ImmutableList<string>.Empty;
        }

        /// <summary>
        /// The list of topics to get metadata for.
        /// </summary>
        public ImmutableList<string> Topics { get; }
    }
}