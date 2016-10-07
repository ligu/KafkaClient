using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// OffsetRequest => ReplicaId [TopicName [Partition Time MaxNumberOfOffsets]]
    ///  ReplicaId => int32   -- The replica id indicates the node id of the replica initiating this request. Normal client consumers should always 
    ///                          specify this as -1 as they have no node id. Other brokers set this to be their own node id. The value -2 is accepted 
    ///                          to allow a non-broker to issue fetch requests as if it were a replica broker for debugging purposes.
    ///  TopicName => string  -- The name of the topic.
    ///  Partition => int32   -- The id of the partition the fetch is for.
    ///  Time => int64        -- Used to ask for all messages before a certain time (ms). There are two special values. Specify -1 to receive the 
    ///                          latest offset (i.e. the offset of the next coming message) and -2 to receive the earliest available offset. Note 
    ///                          that because offsets are pulled in descending order, asking for the earliest offset will always return you a single element.
    ///  MaxNumberOfOffsets => int32 
    /// 
    /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetAPI(AKAListOffset)
    /// A funky Protocol for requesting the starting offset of each segment for the requested partition
    /// </summary>
    public class OffsetRequest : Request, IRequest<OffsetResponse>, IEquatable<OffsetRequest>
    {
        public OffsetRequest(params Offset[] offsets)
            : this((IEnumerable<Offset>)offsets)
        {
        }

        public OffsetRequest(IEnumerable<Offset> offsets) 
            : base(ApiKeyRequestType.Offset)
        {
            Offsets = ImmutableList<Offset>.Empty.AddNotNullRange(offsets);
        }

        public IImmutableList<Offset> Offsets { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as OffsetRequest);
        }

        /// <inheritdoc />
        public bool Equals(OffsetRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Offsets.HasEqualElementsInOrder(other.Offsets);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return Offsets?.GetHashCode() ?? 0;
        }

        /// <inheritdoc />
        public static bool operator ==(OffsetRequest left, OffsetRequest right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(OffsetRequest left, OffsetRequest right)
        {
            return !Equals(left, right);
        }
    }
}