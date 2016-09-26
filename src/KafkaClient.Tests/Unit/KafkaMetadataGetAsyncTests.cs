using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Connection;
using KafkaClient.Protocol;
using KafkaClient.Tests.Helpers;
using Moq;
using NSubstitute;
using NUnit.Framework;

namespace KafkaClient.Tests.Unit
{
    [Category("Unit")]
    [TestFixture]
    public class KafkaMetadataGetAsyncTests
    {
        private ILog _log;

        [SetUp]
        public void Setup()
        {
            _log = Substitute.For<ILog>();
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [TestCase(ErrorResponseCode.LeaderNotAvailable)]
        [TestCase(ErrorResponseCode.OffsetsLoadInProgress)]
        [TestCase(ErrorResponseCode.ConsumerCoordinatorNotAvailable)]
        public async Task ShouldRetryWhenReceiveAnRetryErrorCode(ErrorResponseCode errorCode)
        {
            var conn = Substitute.For<IConnection>();

            conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), It.IsAny<CancellationToken>())
                .Returns(x => CreateMetadataResponse(errorCode), x => CreateMetadataResponse(errorCode));

            var response = await new MetadataRequest("Test").GetAsync(new[] { conn }, _log, CancellationToken.None);

            Received.InOrder(() =>
            {
                conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), It.IsAny<CancellationToken>());
                _log.WarnFormat("Backing off metadata request retry.  Waiting for {0}ms.", 100);
                conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), It.IsAny<CancellationToken>());
            });
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task ShouldRetryWhenReceiveBrokerIdNegativeOne()
        {
            var conn = Substitute.For<IConnection>();

            conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), It.IsAny<CancellationToken>(), Arg.Any<IRequestContext>())
                .Returns(x => CreateMetadataResponse(-1, "123", 1), x => CreateMetadataResponse(ErrorResponseCode.NoError));

            var response = await new MetadataRequest("Test").GetAsync(new[] { conn }, _log, CancellationToken.None);

            Received.InOrder(() =>
            {
                conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), It.IsAny<CancellationToken>());
                _log.WarnFormat("Backing off metadata request retry.  Waiting for {0}ms.", 100);
                conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), It.IsAny<CancellationToken>());
            });
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void ShouldReturnWhenNoErrorReceived()
        {
            var conn = Substitute.For<IConnection>();

            conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), It.IsAny<CancellationToken>())
                .Returns(x => CreateMetadataResponse(ErrorResponseCode.NoError));

            var source = new CancellationTokenSource();
            var response = new MetadataRequest("Test").GetAsync(new[] { conn }, _log, source.Token);
            source.Cancel();

            conn.Received(1).SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>());
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void ShouldReturnWhenNoErrorReceivedAndTopicsNotSpecified()
        {
            var conn = Substitute.For<IConnection>();

            conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), It.IsAny<CancellationToken>())
                .Returns(x => CreateMetadataResponse(ErrorResponseCode.NoError));

            var source = new CancellationTokenSource();
            var response = new MetadataRequest().GetAsync(new[] { conn }, _log, source.Token);
            source.Cancel();

            conn.Received(1).SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>());
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [TestCase(ErrorResponseCode.Unknown)]
        [TestCase(ErrorResponseCode.RequestTimedOut)]
        [TestCase(ErrorResponseCode.InvalidMessage)]
        [ExpectedException(typeof(RequestException))]
        public async Task ShouldThrowExceptionWhenNotARetriableErrorCode(ErrorResponseCode errorCode)
        {
            var conn = Substitute.For<IConnection>();

            conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), It.IsAny<CancellationToken>()).Returns(x => CreateMetadataResponse(errorCode));

            var response = await new MetadataRequest("Test").GetAsync(new[] { conn }, _log, CancellationToken.None);
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [TestCase(null)]
        [TestCase("")]
        [ExpectedException(typeof(ConnectionException))]
        public async Task ShouldThrowExceptionWhenHostIsMissing(string host)
        {
            var conn = Substitute.For<IConnection>();

            conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), It.IsAny<CancellationToken>()).Returns(x => CreateMetadataResponse(1, host, 1));

            var response = await new MetadataRequest("Test").GetAsync(new[] { conn }, _log, CancellationToken.None);
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [TestCase(0)]
        [TestCase(-1)]
        [ExpectedException(typeof(ConnectionException))]
        public async Task ShouldThrowExceptionWhenPortIsMissing(int port)
        {
            var conn = Substitute.For<IConnection>();

            conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), It.IsAny<CancellationToken>()).Returns(x => CreateMetadataResponse(1, "123", port));

            var response = await new MetadataRequest("Test").GetAsync(new[] { conn }, _log, CancellationToken.None);
        }

#pragma warning disable 1998
        private Task<MetadataResponse> CreateMetadataResponse(int brokerId, string host, int port)
        {
            var tcs = new TaskCompletionSource<MetadataResponse>();
            tcs.SetResult(new MetadataResponse(new [] { new Broker(brokerId, host, port) }, new MetadataTopic[] {}));
            return tcs.Task;
        }

        private async Task<MetadataResponse> CreateMetadataResponse(ErrorResponseCode errorCode)
        {
            return new MetadataResponse(new Broker[] {}, new [] { new MetadataTopic("Test", errorCode, new MetadataPartition[] {})});
        }
#pragma warning restore 1998
    }
}