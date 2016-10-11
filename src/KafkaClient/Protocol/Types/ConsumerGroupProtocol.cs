using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol.Types
{
    /// <summary>
    /// ProtocolType => "consumer"
    ///  
    /// ProtocolName => AssignmentStrategy
    ///   AssignmentStrategy => string
    ///  
    /// ProtocolMetadata => Version Subscription UserData
    ///   Version => int16
    ///   Subscription => [Topic]
    ///     Topic => string
    ///   UserData => bytes    
    /// see http://kafka.apache.org/protocol.html#protocol_messages for details
    /// 
    /// The UserData field can be used by custom partition assignment strategies. For example, in a sticky partitioning implementation, this 
    /// field can contain the assignment from the previous generation. In a resource-based assignment strategy, it could include the number of 
    /// cpus on the machine hosting each consumer instance.
    /// 
    /// Kafka Connect uses the "connect" protocol type and its protocol details are internal to the Connect implementation.
    /// </summary>
    public class ConsumerGroupProtocolMetadata : IMemberMetadata
    {
        private static readonly byte[] Empty = {};

        public ConsumerGroupProtocolMetadata(short version = 0, IEnumerable<string> topicNames = null, byte[] userData = null)
        {
            Version = version;
            Subscription = ImmutableList<string>.Empty.AddNotNullRange(topicNames);
            UserData = userData ?? Empty;
        }

        public short Version { get; }

        /// <summary>
        /// The topics subscribed to by this consumer group.
        /// </summary>
        public IImmutableList<string> Subscription { get; }

        public byte[] UserData { get; }
    }
}