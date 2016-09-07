using System;
using System.Collections.Generic;

namespace KafkaNet.Protocol
{
    /// <summary>
    /// Class that represents the api call to commit a specific set of offsets for a given topic.  The offset is saved under the
    /// arbitrary ConsumerGroup name provided by the call.
    /// </summary>
    public class OffsetCommitRequest : BaseRequest, IKafkaRequest<OffsetCommitResponse>
    {
        public ApiKeyRequestType ApiKey => ApiKeyRequestType.OffsetCommit;

        /// <summary>
        /// The generation of the consumer group.
        /// Only version 1 (0.8.2) and above
        /// </summary>
        public int GenerationId { get; set; }

        /// <summary>
        /// The consumer id assigned by the group coordinator.
        /// Only version 1 (0.8.2) and above
        /// </summary>
        public string MemberId { get; set; }

        /// <summary>
        /// The consumer group id.
        /// </summary>
        public string ConsumerGroup { get; set; }

        /// <summary>
        /// Time period to retain the offset.
        /// </summary>
        public TimeSpan? OffsetRetention { get; set; }

        public List<OffsetCommit> OffsetCommits { get; set; }

        public KafkaDataPayload Encode()
        {
            if (OffsetCommits == null) {
                OffsetCommits = new List<OffsetCommit>();
            }

            return new KafkaDataPayload {
                Buffer = EncodeRequest.OffsetCommitRequest(this),
                CorrelationId = CorrelationId,
                ApiKey = ApiKey
            };
        }

        public OffsetCommitResponse Decode(byte[] payload)
        {
            return DecodeResponse.OffsetCommitResponse(ApiVersion, payload);
        }
    }
}