using System.Collections.Generic;
using System.Linq;

namespace KafkaNet.Protocol
{
    public class ProduceRequest : BaseRequest, IKafkaRequest<ProduceResponse>
    {
        /// <summary>
        /// Provide a hint to the broker call not to expect a response for requests without Acks.
        /// </summary>
        public override bool ExpectResponse => Acks != 0;

        /// <summary>
        /// Indicates the type of kafka encoding this request is.
        /// </summary>
        public ApiKeyRequestType ApiKey => ApiKeyRequestType.Produce;

        /// <summary>
        /// Time kafka will wait for requested ack level before returning.
        /// </summary>
        public int TimeoutMS { get; set; } = 1000;

        /// <summary>
        /// Level of ack required by kafka.  0 immediate, 1 written to leader, 2+ replicas synced, -1 all replicas
        /// </summary>
        public short Acks { get; set; } = 1;

        /// <summary>
        /// Collection of payloads to post to kafka
        /// </summary>
        public List<Payload> Payload = new List<Payload>();

        public KafkaDataPayload Encode()
        {
            return new KafkaDataPayload {
                Buffer = Protocol.EncodeRequest.ProduceRequest(this),
                CorrelationId = CorrelationId,
                MessageCount = Payload.Sum(x => x.Messages.Count)
            };
        }

        public ProduceResponse Decode(byte[] payload)
        {
            return Protocol.DecodeResponse.ProduceResponse(ApiVersion, payload);
        }
    }
}