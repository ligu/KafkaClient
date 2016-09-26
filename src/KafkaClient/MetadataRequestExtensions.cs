using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Connection;
using KafkaClient.Protocol;

namespace KafkaClient
{
    public static class MetadataRequestExtensions
    {
        /// <summary>
        /// Given a collection of server connections, query for the topic metadata.
        /// </summary>
        /// <param name="request">The request to make</param>
        /// <param name="connections">The server connections to query.  Will cycle through the collection, starting at zero until a response is received.</param>
        /// <param name="log">The log</param>
        /// <param name="cancellationToken">Token for cancelling async action.</param>
        /// <returns>MetadataResponse validated to be complete.</returns>
        public static async Task<MetadataResponse> GetAsync(this MetadataRequest request, IEnumerable<IConnection> connections, ILog log, CancellationToken cancellationToken)
        {
            const int maxRetryAttempt = 2;
            bool performRetry;
            var retryAttempt = 0;
            MetadataResponse metadataResponse;

            var connectionList = ImmutableList<IConnection>.Empty.AddNotNullRange(connections);
            do {
                performRetry = false;
                metadataResponse = await GetMetadataResponseAsync(connectionList, request, log, cancellationToken);
                if (metadataResponse == null) return null;

                foreach (var validation in ValidateResponse(metadataResponse)) {
                    switch (validation.Status) {
                        case ValidationResult.Retry:
                            performRetry = true;
                            log.WarnFormat(validation.Message);
                            break;

                        case ValidationResult.Error:
                            if (validation.ErrorCode == ErrorResponseCode.NoError) throw new ConnectionException(validation.Message);

                            throw new RequestException(request.ApiKey, validation.ErrorCode, validation.Message);
                    }
                }

                await BackoffOnRetryAsync(++retryAttempt, performRetry, log, cancellationToken).ConfigureAwait(false);
            } while (retryAttempt < maxRetryAttempt && !cancellationToken.IsCancellationRequested && performRetry);

            return metadataResponse;
        }

        private const int BackoffMilliseconds = 100;

        private static async Task BackoffOnRetryAsync(int retryAttempt, bool performRetry, ILog log, CancellationToken cancellationToken)
        {
            if (performRetry && retryAttempt > 0)
            {
                var backoff = retryAttempt * retryAttempt * BackoffMilliseconds;
                log.WarnFormat("Backing off metadata request retry.  Waiting for {0}ms.", backoff);
                await Task.Delay(TimeSpan.FromMilliseconds(backoff), cancellationToken).ConfigureAwait(false);
            }
        }

        private static async Task<MetadataResponse> GetMetadataResponseAsync(IEnumerable<IConnection> connections, MetadataRequest request, ILog log, CancellationToken cancellationToken)
        {
            var servers = "";
            foreach (var conn in connections) {
                try {
                    servers += " " + conn.Endpoint;
                    return await conn.SendAsync(request, cancellationToken).ConfigureAwait(false);
                } catch (Exception ex) {
                    log.WarnFormat(ex, "Failed to contact {0}. Trying next server", conn.Endpoint);
                }
            }

            throw new RequestException(request.ApiKey, ErrorResponseCode.NoError, $"Unable to query for metadata from any of the provided Kafka servers {servers}");
        }

        private static IEnumerable<MetadataValidationResult> ValidateResponse(MetadataResponse metadata)
        {
            foreach (var broker in metadata.Brokers) {
                yield return ValidateBroker(broker);
            }
            foreach (var topic in metadata.Topics) {
                yield return ValidateTopic(topic);
            }
        }

        private static MetadataValidationResult ValidateBroker(Broker broker)
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

        private static MetadataValidationResult ValidateTopic(MetadataTopic topic)
        {
            var errorCode = topic.ErrorCode;
            try
            {
                if (errorCode == ErrorResponseCode.NoError) return new MetadataValidationResult();

                switch (errorCode)
                {
                    case ErrorResponseCode.LeaderNotAvailable:
                    case ErrorResponseCode.OffsetsLoadInProgress:
                    case ErrorResponseCode.ConsumerCoordinatorNotAvailable:
                        return new MetadataValidationResult {
                            Status = ValidationResult.Retry,
                            ErrorCode = errorCode,
                            Message = $"topic/{topic.TopicName} returned error code of {errorCode}: Retrying"
                        };
                }

                return new MetadataValidationResult {
                    Status = ValidationResult.Error,
                    ErrorCode = errorCode,
                    Message = $"topic/{topic.TopicName} returned an error of {errorCode}"
                };
            }
            catch
            {
                return new MetadataValidationResult
                {
                    Status = ValidationResult.Error,
                    ErrorCode = ErrorResponseCode.Unknown,
                    Message = $"topic/{topic.TopicName} returned unknown error of {errorCode} in metadata response"
                };
            }
        }

        private enum ValidationResult
        {
            Valid,
            Error,
            Retry
        }

        private class MetadataValidationResult
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
}