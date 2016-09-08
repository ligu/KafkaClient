using KafkaNet.Protocol;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace KafkaNet
{
    /// <summary>
    /// This provider blocks while it attempts to get the MetaData configuration of the Kafka servers.  If any retry errors occurs it will
    /// continue to block the downstream call and then repeatedly query kafka until the retry errors subside.  This repeat call happens in
    /// a backoff manner, which each subsequent call waiting longer before a requery.
    ///
    /// Error Codes:
    /// LeaderNotAvailable = 5
    /// NotLeaderForPartition = 6
    /// ConsumerCoordinatorNotAvailable = 15
    /// BrokerId = -1
    ///
    /// Documentation:
    /// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-MetadataResponse
    /// </summary>
    public class KafkaMetadataProvider : IDisposable
    {
        private const int BackoffMilliseconds = 100;

        private readonly IKafkaLog _log;

        private bool _interrupted;

        public KafkaMetadataProvider(IKafkaLog log)
        {
            _log = log;
        }

        /// <summary>
        /// Given a collection of server connections, query for the topic metadata.
        /// </summary>
        /// <param name="connections">The server connections to query.  Will cycle through the collection, starting at zero until a response is received.</param>
        /// <param name="topics">The collection of topics to get metadata for.</param>
        /// <returns>MetadataResponse validated to be complete.</returns>
        public Task<MetadataResponse> Get(IKafkaConnection[] connections, IEnumerable<string> topics)
        {
            var request = new MetadataRequest { Topics = topics.ToList() };
            if (request.Topics.Count <= 0) return null;
            return Get(connections, request);
        }

        /// <summary>
        /// Given a collection of server connections, query for all topics metadata.
        /// </summary>
        /// <param name="connections">The server connections to query.  Will cycle through the collection, starting at zero until a response is received.</param>
        /// <returns>MetadataResponse validated to be complete.</returns>
        public Task<MetadataResponse> Get(IKafkaConnection[] connections)
        {
            var request = new MetadataRequest();
            return Get(connections, request);
        }

        private async Task<MetadataResponse> Get(IKafkaConnection[] connections, MetadataRequest request)
        {
            var maxRetryAttempt = 2;
            var performRetry = false;
            var retryAttempt = 0;
            MetadataResponse metadataResponse = null;

            do
            {
                performRetry = false;
                metadataResponse = await GetMetadataResponse(connections, request);
                if (metadataResponse == null) return null;

                foreach (var validation in ValidateResponse(metadataResponse))
                {
                    switch (validation.Status)
                    {
                        case ValidationResult.Retry:
                            performRetry = true;
                            _log.WarnFormat(validation.Message);
                            break;

                        case ValidationResult.Error:
                            if (validation.ErrorCode == ErrorResponseCode.NoError) throw new KafkaConnectionException(validation.Message);

                            throw new KafkaRequestException(request.ApiKey, validation.ErrorCode, validation.Message);
                    }
                }

                await BackoffOnRetry(++retryAttempt, performRetry).ConfigureAwait(false);
            } while (retryAttempt < maxRetryAttempt && _interrupted == false && performRetry);

            return metadataResponse;
        }

        private async Task BackoffOnRetry(int retryAttempt, bool performRetry)
        {
            if (performRetry && retryAttempt > 0)
            {
                var backoff = retryAttempt * retryAttempt * BackoffMilliseconds;
                _log.WarnFormat("Backing off metadata request retry.  Waiting for {0}ms.", backoff);
                await Task.Delay(TimeSpan.FromMilliseconds(backoff)).ConfigureAwait(false);
            }
        }

        private async Task<MetadataResponse> GetMetadataResponse(IKafkaConnection[] connections, MetadataRequest request)
        {
            //try each default broker until we find one that is available
            foreach (var conn in connections)
            {
                try
                {
                    var response = await conn.SendAsync(request).ConfigureAwait(false);
                    if (response?.Topics.Count > 0) {
                        return response;
                    }
                }
                catch (Exception ex)
                {
                    _log.WarnFormat("Failed to contact Kafka server={0}. Trying next default server. Exception={1}", conn.Endpoint, ex);
                }
            }

            throw new KafkaRequestException(request.ApiKey, ErrorResponseCode.NoError, $"Unable to query for metadata from any of the provided Kafka servers. Server list: {string.Join(", ", connections.Select(x => x.ToString()))}");
        }

        private IEnumerable<MetadataValidationResult> ValidateResponse(MetadataResponse metadata)
        {
            foreach (var broker in metadata.Brokers) {
                yield return ValidateBroker(broker);
            }
            foreach (var topic in metadata.Topics) {
                yield return ValidateTopic(topic);
            }
        }

        private MetadataValidationResult ValidateBroker(Broker broker)
        {
            if (broker.BrokerId == -1) {
                return new MetadataValidationResult { Status = ValidationResult.Retry, ErrorCode = ErrorResponseCode.Unknown };
            }

            if (string.IsNullOrEmpty(broker.Host)) {
                return new MetadataValidationResult {
                    Status = ValidationResult.Error,
                    ErrorCode = ErrorResponseCode.NoError,
                    Message = "Broker missing host information."
                };
            }

            if (broker.Port <= 0) {
                return new MetadataValidationResult {
                    Status = ValidationResult.Error,
                    ErrorCode = ErrorResponseCode.NoError,
                    Message = "Broker missing port information."
                };
            }

            return new MetadataValidationResult();
        }

        private MetadataValidationResult ValidateTopic(MetadataTopic topic)
        {
            try
            {
                var errorCode = (ErrorResponseCode)topic.ErrorCode;

                if (errorCode == ErrorResponseCode.NoError) return new MetadataValidationResult();

                switch (errorCode)
                {
                    case ErrorResponseCode.LeaderNotAvailable:
                    case ErrorResponseCode.OffsetsLoadInProgress:
                    case ErrorResponseCode.ConsumerCoordinatorNotAvailable:
                        return new MetadataValidationResult {
                            Status = ValidationResult.Retry,
                            ErrorCode = errorCode,
                            Message = $"Topic:{topic.Name} returned error code of {errorCode}. Retrying."
                        };
                }

                return new MetadataValidationResult {
                    Status = ValidationResult.Error,
                    ErrorCode = errorCode,
                    Message = $"Topic:{topic.Name} returned an error of {errorCode}"
                };
            }
            catch
            {
                return new MetadataValidationResult
                {
                    Status = ValidationResult.Error,
                    ErrorCode = ErrorResponseCode.Unknown,
                    Message = $"Unknown error code returned in metadata response.  ErrorCode: {topic.ErrorCode}"
                };
            }
        }

        public void Dispose()
        {
            _interrupted = true;
        }
    }

    public enum ValidationResult
    {
        Valid,
        Error,
        Retry
    }

    public class MetadataValidationResult
    {
        public ValidationResult Status { get; set; }
        public string Message { get; set; }
        public ErrorResponseCode ErrorCode { get; set; }

        public MetadataValidationResult()
        {
            ErrorCode = ErrorResponseCode.NoError;
            Status = ValidationResult.Valid;
            Message = "";
        }
    }
}