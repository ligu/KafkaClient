using kafka_tests.Helpers;
using KafkaNet;
using KafkaNet.Protocol;
using NSubstitute;
using NUnit.Framework;
using System.Threading.Tasks;

namespace kafka_tests.Unit
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

            conn.SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>())
                .Returns(x => CreateMetadataResponse(errorCode), x => CreateMetadataResponse(errorCode));

            using (var provider = new KafkaMetadataProvider(_log))
            {
                var response = await provider.Get(new[] { conn }, new[] { "Test" });
            }

            Received.InOrder(() =>
            {
                conn.SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>());
                _log.WarnFormat("Backing off metadata request retry.  Waiting for {0}ms.", 100);
                conn.SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>());
            });
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task ShouldRetryWhenReceiveBrokerIdNegativeOne()
        {
            var conn = Substitute.For<IKafkaConnection>();

            conn.SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>())
                .Returns(x => CreateMetadataResponse(-1, "123", 1), x => CreateMetadataResponse(ErrorResponseCode.NoError));

            using (var provider = new KafkaMetadataProvider(_log))
            {
                var response = await provider.Get(new[] { conn }, new[] { "Test" });
            }

            Received.InOrder(() =>
            {
                conn.SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>());
                _log.WarnFormat("Backing off metadata request retry.  Waiting for {0}ms.", 100);
                conn.SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>());
            });
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void ShouldReturnWhenNoErrorReceived()
        {
            var conn = Substitute.For<IKafkaConnection>();

            conn.SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>())
                .Returns(x => CreateMetadataResponse(ErrorResponseCode.NoError));

            using (var provider = new KafkaMetadataProvider(_log))
            {
                var response = provider.Get(new[] { conn }, new[] { "Test" });
            }

            conn.Received(1).SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>());
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void ShouldReturnWhenNoErrorReceivedAndTopicsNotSpecified()
        {
            var conn = Substitute.For<IKafkaConnection>();

            conn.SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>())
                .Returns(x => CreateMetadataResponse(ErrorResponseCode.NoError));

            using (var provider = new KafkaMetadataProvider(_log))
            {
                var response = provider.Get(new[] { conn });
            }

            conn.Received(1).SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>());
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [TestCase(ErrorResponseCode.Unknown)]
        [TestCase(ErrorResponseCode.RequestTimedOut)]
        [TestCase(ErrorResponseCode.InvalidMessage)]
        [ExpectedException(typeof(KafkaRequestException))]
        public async Task ShouldThrowExceptionWhenNotARetriableErrorCode(ErrorResponseCode errorCode)
        {
            var conn = Substitute.For<IKafkaConnection>();

            conn.SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>()).Returns(x => CreateMetadataResponse(errorCode));

            using (var provider = new KafkaMetadataProvider(_log))
            {
                var response = await provider.Get(new[] { conn }, new[] { "Test" });
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [TestCase(null)]
        [TestCase("")]
        [ExpectedException(typeof(KafkaConnectionException))]
        public async Task ShouldThrowExceptionWhenHostIsMissing(string host)
        {
            var conn = Substitute.For<IKafkaConnection>();

            conn.SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>()).Returns(x => CreateMetadataResponse(1, host, 1));

            using (var provider = new KafkaMetadataProvider(_log))
            {
                var response = await provider.Get(new[] { conn }, new[] { "Test" });
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [TestCase(0)]
        [TestCase(-1)]
        [ExpectedException(typeof(KafkaConnectionException))]
        public async Task ShouldThrowExceptionWhenPortIsMissing(int port)
        {
            var conn = Substitute.For<IKafkaConnection>();

            conn.SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>()).Returns(x => CreateMetadataResponse(1, "123", port));

            using (var provider = new KafkaMetadataProvider(_log))
            {
                var response = await provider.Get(new[] { conn }, new[] { "Test" });
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