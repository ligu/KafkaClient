using System;
using System.Threading;

namespace KafkaClient.Common
{
    public class MergedCancellation : IDisposable
    {
        private readonly CancellationTokenSource _source;
        private readonly CancellationTokenRegistration[] _registrations;

        public MergedCancellation(params CancellationToken[] tokens)
        {
            _source = new CancellationTokenSource();
            _registrations = new CancellationTokenRegistration[tokens.Length];
            for (var i = 0; i < tokens.Length; i++) {
                _registrations[i] = tokens[i].Register(() => _source.Cancel());
            }
        }

        public CancellationToken Token => _source.Token;

        public void Dispose()
        {
            using (_source) {
                foreach (var registration in _registrations) {
                    using (registration) {
                    }
                }
            }
        }
    }
}