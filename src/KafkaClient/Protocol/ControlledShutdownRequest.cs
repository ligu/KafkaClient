using System;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// ControlledShutdown Request => broker_id 
    ///  broker_id => INT32
    /// </summary>
    public class ControlledShutdownRequest : Request, IRequest<ControlledShutdownResponse>, IEquatable<ControlledShutdownRequest>
    {
        public ControlledShutdownRequest(int brokerId) 
            : base(ApiKeyRequestType.ControlledShutdown)
        {
            BrokerId = brokerId;
        }

        public int BrokerId { get; }

        public override bool Equals(object obj)
        {
            return Equals(obj as ControlledShutdownRequest);
        }

        public bool Equals(ControlledShutdownRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                && BrokerId == other.BrokerId;
        }

        public override int GetHashCode()
        {
            unchecked {
                return (base.GetHashCode() * 397) ^ BrokerId;
            }
        }

        public static bool operator ==(ControlledShutdownRequest left, ControlledShutdownRequest right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(ControlledShutdownRequest left, ControlledShutdownRequest right)
        {
            return !Equals(left, right);
        }
    }
}