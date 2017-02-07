using System;
// ReSharper disable InconsistentNaming

namespace KafkaClient.Protocol
{
    /// <summary>
    /// Heartbeat Request (Version: 0) => group_id group_generation_id member_id 
    ///   group_id => STRING           -- The group id.
    ///   group_generation_id => INT32 -- The generation of the group.
    ///   member_id => STRING          -- The member id assigned by the group coordinator.
    /// 
    /// see http://kafka.apache.org/protocol.html#protocol_messages
    /// 
    /// Once a member has joined and synced, it will begin sending periodic heartbeats to keep itself in the group. If a heartbeat has *not* been 
    /// received by the coordinator with the configured session timeout, the member will be kicked out of the group.
    /// </summary>
    public class HeartbeatRequest : GroupRequest, IRequest<HeartbeatResponse>
    {
        public override string ToString() => $"{{Api:{ApiKey},group_id:{group_id},member_id:{member_id},generation_id:{generation_id}}}";

        public override string ShortString() => $"{ApiKey} {group_id} {member_id}";

        protected override void EncodeBody(IKafkaWriter writer, IRequestContext context)
        {
            writer.Write(group_id)
                  .Write(generation_id)
                  .Write(member_id);
        }

        public HeartbeatResponse ToResponse(IRequestContext context, ArraySegment<byte> bytes) => HeartbeatResponse.FromBytes(context, bytes);

        /// <inheritdoc />
        public HeartbeatRequest(string groupId, int generationId, string memberId) 
            : base(ApiKey.Heartbeat, groupId, memberId, generationId)
        {
        }
    }
}