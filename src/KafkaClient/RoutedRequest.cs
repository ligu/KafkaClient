using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Connections;
using KafkaClient.Protocol;

namespace KafkaClient
{
    internal abstract class RoutedRequest<T> where T : class, IResponse
    {
        protected RoutedRequest(IRequest<T> request, ILog log)
        {
            _request = request;
            _log = log;
        }

        private readonly ILog _log;
        private readonly IRequest<T> _request;

        private IConnection _connection;
        private CancellationToken _cancellationToken;
        private IRouter _router;
        private T _response;

        public async Task SendAsync(IRouter router, CancellationToken cancellationToken, IRequestContext context = null)
        {
            try {
                _response = null;
                _router = router;
                _cancellationToken = cancellationToken;
                _connection = await GetConnectionAsync(router, cancellationToken).ConfigureAwait(false);
                _response = _connection == null
                    ? null
                    : await _connection.SendAsync(_request, cancellationToken, context).ConfigureAwait(false);
            } finally {
                ReturnConnection(router, _connection);
            }
        }

        protected abstract Task<IConnection> GetConnectionAsync(IRouter router, CancellationToken cancellationToken);

        protected virtual void ReturnConnection(IRouter router, IConnection connection)
        {
        }

        public RetryAttempt<T> MetadataRetryResponse(int attempt, out bool? metadataInvalid)
        {
            metadataInvalid = false;
            if (_response == null) return _request.ExpectResponse ? RetryAttempt<T>.Retry : new RetryAttempt<T>(null);

            var errors = _response.Errors.Where(e => e != ErrorCode.None).ToList();
            if (errors.Count == 0) return new RetryAttempt<T>(_response);

            var shouldRetry = errors.Any(e => e.IsRetryable());
            metadataInvalid = errors.All(e => e.IsFromStaleMetadata());
            _log.Warn(() => LogEvent.Create($"{_request.ApiKey} response contained errors (attempt {attempt}): {string.Join(" ", errors)}"));

            if (!shouldRetry) ThrowExtractedException();
            return RetryAttempt<T>.Retry;
        }

        private bool TryReconnect(Exception exception) => exception is ObjectDisposedException && (_router?.TryRestore(_connection, _cancellationToken) ?? false);

        public void OnRetry(Exception exception, out bool? shouldRetry)
        {
            shouldRetry = true;
            if (exception != null) {
                if (!TryReconnect(exception) && !exception.IsPotentiallyRecoverableByMetadataRefresh()) throw exception.PrepareForRethrow();
                shouldRetry = null; // ie. the state of the metadata is unknown
                if (!(exception is OperationCanceledException)) {
                    _log.Debug(() => LogEvent.Create(exception));
                }
            }
        }

        public void LogAttempt(int retryAttempt)
        {
            if (retryAttempt > 0) {
                _log.Debug(() => LogEvent.Create($"Retrying request {_request.ApiKey} (attempt {retryAttempt})"));
            }
        }

        public void ThrowExtractedException()
        {
            if (_response == null || _response.Errors.All(e => e.IsSuccess())) return;
            throw ResponseException;
        }

        public Exception ResponseException => _request.ExtractExceptions(_response, _connection?.Endpoint);
    }
}