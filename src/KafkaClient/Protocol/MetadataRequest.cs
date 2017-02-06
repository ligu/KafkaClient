using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// Metadata Request => [topics]
    ///  topic => STRING  -- An array of topics to fetch metadata for. If the topics array is null fetch metadata for all topics.
    ///
    /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-MetadataAPI
    /// </summary>
    public class MetadataRequest : Request, IRequest<MetadataResponse>, IEquatable<MetadataRequest>
    {
        public override string ToString() => $"{{Api:{ApiKey},topics:[{topics.ToStrings()}]}}";

        public override string ShortString() => topics.Count == 1 ? $"{ApiKey} {topics[0]}" : ApiKey.ToString();

        protected override void EncodeBody(IKafkaWriter writer, IRequestContext context)
        {
            writer.Write(topics, true);
        }

        public MetadataRequest(string topic)
            : this (new []{topic})
        {
        }

        public MetadataRequest(IEnumerable<string> topics = null) 
            : base(ApiKey.Metadata)
        {
            this.topics = ImmutableList<string>.Empty.AddNotNullRange(topics);
        }

        /// <summary>
        /// The list of topics to get metadata for.
        /// </summary>
        public IImmutableList<string> topics { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as MetadataRequest);
        }

        /// <inheritdoc />
        public bool Equals(MetadataRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return topics.HasEqualElementsInOrder(other.topics);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return topics?.Count.GetHashCode() ?? 0;
        }

        #endregion
    }
}