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
    public class KafkaMetadataProviderTests
    {
        private IKafkaLog _log;

        [SetUp]
        public void Setup()
        {
            _log = Substitute.For<IKafkaLog>();
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [TestCase(ErrorResponseCode.LeaderNotAvailable)]
        [TestCase(ErrorResponseCode.OffsetsLoadInProgress)]
        [TestCase(ErrorResponseCode.ConsumerCoordinatorNotAvailable)]
        public async Task ShouldRetryWhenReceiveAnRetryErrorCode(ErrorResponseCode errorCode)
        {
            var conn = Substitute.For<IKafkaConnection>();

            conn.SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>(), It.IsAny<CancellationToken>())
                .Returns(x => CreateMetadataResponse(errorCode), x => CreateMetadataResponse(errorCode));

            using (var provider = new KafkaMetadataProvider(_log))
            {
                var response = await provider.GetAsync(new[] { conn }, new[] { "Test" }, CancellationToken.None);
            }

            Received.InOrder(() =>
            {
                conn.SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>(), It.IsAny<CancellationToken>());
                _log.WarnFormat("Backing off metadata request retry.  Waiting for {0}ms.", 100);
                conn.SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>(), It.IsAny<CancellationToken>());
            });
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task ShouldRetryWhenReceiveBrokerIdNegativeOne()
        {
            var conn = Substitute.For<IKafkaConnection>();

            conn.SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>(), It.IsAny<CancellationToken>(), Arg.Any<IRequestContext>())
                .Returns(x => CreateMetadataResponse(-1, "123", 1), x => CreateMetadataResponse(ErrorResponseCode.NoError));

            using (var provider = new KafkaMetadataProvider(_log))
            {
                var response = await provider.GetAsync(new[] { conn }, new[] { "Test" }, CancellationToken.None);
            }

            Received.InOrder(() =>
            {
                conn.SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>(), It.IsAny<CancellationToken>());
                _log.WarnFormat("Backing off metadata request retry.  Waiting for {0}ms.", 100);
                conn.SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>(), It.IsAny<CancellationToken>());
            });
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void ShouldReturnWhenNoErrorReceived()
        {
            var conn = Substitute.For<IKafkaConnection>();

            conn.SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>(), It.IsAny<CancellationToken>())
                .Returns(x => CreateMetadataResponse(ErrorResponseCode.NoError));

            using (var provider = new KafkaMetadataProvider(_log))
            {
                var response = provider.GetAsync(new[] { conn }, new[] { "Test" }, CancellationToken.None);
            }

            conn.Received(1).SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>(), It.IsAny<CancellationToken>());
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void ShouldReturnWhenNoErrorReceivedAndTopicsNotSpecified()
        {
            var conn = Substitute.For<IKafkaConnection>();

            conn.SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>(), It.IsAny<CancellationToken>())
                .Returns(x => CreateMetadataResponse(ErrorResponseCode.NoError));

            using (var provider = new KafkaMetadataProvider(_log))
            {
                var response = provider.GetAsync(new[] { conn }, CancellationToken.None);
            }

            conn.Received(1).SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>(), It.IsAny<CancellationToken>());
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [TestCase(ErrorResponseCode.Unknown)]
        [TestCase(ErrorResponseCode.RequestTimedOut)]
        [TestCase(ErrorResponseCode.InvalidMessage)]
        [ExpectedException(typeof(KafkaRequestException))]
        public async Task ShouldThrowExceptionWhenNotARetriableErrorCode(ErrorResponseCode errorCode)
        {
            var conn = Substitute.For<IKafkaConnection>();

            conn.SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>(), It.IsAny<CancellationToken>()).Returns(x => CreateMetadataResponse(errorCode));

            using (var provider = new KafkaMetadataProvider(_log))
            {
                var response = await provider.GetAsync(new[] { conn }, new[] { "Test" }, CancellationToken.None);
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [TestCase(null)]
        [TestCase("")]
        [ExpectedException(typeof(KafkaConnectionException))]
        public async Task ShouldThrowExceptionWhenHostIsMissing(string host)
        {
            var conn = Substitute.For<IKafkaConnection>();

            conn.SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>(), It.IsAny<CancellationToken>()).Returns(x => CreateMetadataResponse(1, host, 1));

            using (var provider = new KafkaMetadataProvider(_log))
            {
                var response = await provider.GetAsync(new[] { conn }, new[] { "Test" }, CancellationToken.None);
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [TestCase(0)]
        [TestCase(-1)]
        [ExpectedException(typeof(KafkaConnectionException))]
        public async Task ShouldThrowExceptionWhenPortIsMissing(int port)
        {
            var conn = Substitute.For<IKafkaConnection>();

            conn.SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>(), It.IsAny<CancellationToken>()).Returns(x => CreateMetadataResponse(1, "123", port));

            using (var provider = new KafkaMetadataProvider(_log))
            {
                var response = await provider.GetAsync(new[] { conn }, new[] { "Test" }, CancellationToken.None);
            }
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