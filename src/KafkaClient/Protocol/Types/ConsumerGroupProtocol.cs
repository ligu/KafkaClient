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
    public class ConsumerGroupProtocol : GroupProtocol
    {
        public const string ProtocolType = "consumer";

        private static readonly byte[] Empty = {};

        /// <inheritdoc />
        public ConsumerGroupProtocol(string name, short version = 0, IEnumerable<string> topicNames = null, byte[] userData = null) : base(name, userData)
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

        ///// <inheritdoc />
        //public override byte[] Metadata => KafkaEncoder.EncodeMetadata(this);
    }

    /// <summary>
    ///  The format of the MemberAssignment field for consumer groups is included below:
    /// MemberAssignment => Version PartitionAssignment
    ///   Version => int16
    ///   PartitionAssignment => [Topic [Partition]]
    ///     Topic => string
    ///     Partition => int32
    ///   UserData => bytes
    /// All client implementations using the "consumer" protocol type should support this schema.
    /// 
    /// from https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
    /// </summary>
    public class ConsumerGroupMemberAssignment
    {
        public ConsumerGroupMemberAssignment(short version, IEnumerable<PartitionAssignment> partitionAssignments)
        {
            Version = version;
            PartitionAssignments = ImmutableList<PartitionAssignment>.Empty.AddNotNullRange(partitionAssignments);
        }

        public short Version { get; }
        public IImmutableList<PartitionAssignment> PartitionAssignments { get; }
    }

    public class PartitionAssignment
    {
        public PartitionAssignment(string topicName, IEnumerable<int> partitionIds)
        {
            TopicName = topicName;
            PartitionIds = ImmutableList<int>.Empty.AddNotNullRange(partitionIds);
        }

        public string TopicName { get; }
        public IImmutableList<int> PartitionIds { get; }
    }
}