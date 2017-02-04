using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    public abstract class TopicsResponse : IResponse, IEquatable<TopicsResponse>
    {
        public override string ToString() => $"{{Topics:[{Topics.ToStrings()}]}}";

        protected TopicsResponse(IEnumerable<Topic> topics = null)
        {
            Topics = ImmutableList<Topic>.Empty.AddNotNullRange(topics);
            Errors = ImmutableList<ErrorCode>.Empty.AddRange(Topics.Select(t => t.ErrorCode));
        }

        public IImmutableList<Topic> Topics { get; } 
        public IImmutableList<ErrorCode> Errors { get; }

        #region Equality

        public override bool Equals(object obj)
        {
            return Equals(obj as TopicsResponse);
        }

        public bool Equals(TopicsResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Topics.HasEqualElementsInOrder(other.Topics);
        }

        public override int GetHashCode()
        {
            return Topics?.Count.GetHashCode() ?? 0;
        }

        #endregion

        public class Topic : IEquatable<Topic>
        {
            public override string ToString() => $"{{TopicName:{TopicName},ErrorCode:{ErrorCode}}}";

            public Topic(string topicName, ErrorCode errorCode)
            {
                TopicName = topicName;
                ErrorCode = errorCode;
            }

            public string TopicName { get; }
            public ErrorCode ErrorCode { get; }

            #region Equality

            public override bool Equals(object obj)
            {
                return Equals(obj as Topic);
            }

            public bool Equals(Topic other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return string.Equals(TopicName, other.TopicName) && ErrorCode == other.ErrorCode;
            }

            public override int GetHashCode()
            {
                unchecked {
                    return ((TopicName?.GetHashCode() ?? 0) * 397) ^ (int) ErrorCode;
                }
            }

            #endregion
        }
    }
}