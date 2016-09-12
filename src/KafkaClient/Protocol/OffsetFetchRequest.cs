using System.Collections.Generic;
using System.Collections.Immutable;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// Class that represents both the request and the response from a kafka server of requesting a stored offset value
    /// for a given consumer group.  Essentially this part of the api allows a user to save/load a given offset position
    /// under any abritrary name.
    /// </summary>
    public class OffsetFetchRequest : KafkaRequest, IKafkaRequest<OffsetFetchResponse>
    {
        public OffsetFetchRequest(string consumerGroup, params Topic[] topics) 
            : this(consumerGroup, (IEnumerable<Topic>)topics)
        {
        }

        public OffsetFetchRequest(string consumerGroup, IEnumerable<Topic> topics) 
            : base(ApiKeyRequestType.OffsetFetch)
        {
            ConsumerGroup = consumerGroup;
            Topics = topics != null ? ImmutableList<Topic>.Empty.AddRange(topics) : ImmutableList<Topic>.Empty;
        }

        public string ConsumerGroup { get; }

        public ImmutableList<Topic> Topics { get; }
    }
}