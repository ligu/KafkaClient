using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// JoinGroup Response (Version: 0) => error_code generation_id group_protocol leader_id member_id [members] 
    ///   error_code => INT16
    ///   generation_id => INT32   -- The generation of the consumer group.
    ///   group_protocol => STRING -- The group protocol selected by the coordinator
    ///   leader_id => STRING      -- The leader of the group
    ///   member_id => STRING      -- The consumer id assigned by the group coordinator.
    ///   members => member_id member_metadata 
    ///     member_id => STRING
    ///     member_metadata => BYTES
    /// 
    /// see http://kafka.apache.org/protocol.html#protocol_messages for details
    /// 
    /// After receiving join group requests from all members in the group, the coordinator will select one member to be the group leader 
    /// and a protocol which is supported by all members. The leader will receive the full list of members along with the associated metadata 
    /// for the protocol chosen. Other members, followers, will receive an empty array of members. It is the responsibility of the leader to 
    /// inspect the metadata of each member and assign state using SyncGroup request below.
    /// 
    /// Upon every completion of the join group phase, the coordinator increments a GenerationId for the group. This is returned as a field in 
    /// the response to each member, and is sent in heartbeats and offset commit requests. When the coordinator rebalances a group, the 
    /// coordinator will send an error code indicating that the member needs to rejoin. If the member does not rejoin before a rebalance 
    /// completes, then it will have an old generationId, which will cause ILLEGAL_GENERATION errors when included in new requests.
    /// </summary>
    public class JoinGroupResponse : IResponse
    {
        public JoinGroupResponse(ErrorResponseCode errorCode, int generationId, string groupProtocol, string leaderId, string memberId, IEnumerable<GroupMember> members)
        {
            ErrorCode = errorCode;
            Errors = ImmutableList<ErrorResponseCode>.Empty.Add(ErrorCode);
            GenerationId = generationId;
            GroupProtocol = groupProtocol;
            LeaderId = leaderId;
            MemberId = memberId;
            Members = ImmutableList<GroupMember>.Empty.AddNotNullRange(members);
        }

        /// <inheritdoc />
        public IImmutableList<ErrorResponseCode> Errors { get; }

        public ErrorResponseCode ErrorCode { get; }

        /// <summary>
        /// The generation counter for completion of the join group phase.
        /// </summary>
        public int GenerationId { get; }

        /// <summary>
        /// The group protocol selected by the coordinator. Is this the name or the type??
        /// </summary>
        public string GroupProtocol { get; }

        /// <summary>
        /// The leader of the group.
        /// </summary>
        public string LeaderId { get; }

        /// <summary>
        /// The consumer id assigned by the group coordinator.
        /// </summary>
        public string MemberId { get; }

        public IImmutableList<GroupMember> Members { get; }

    }
}