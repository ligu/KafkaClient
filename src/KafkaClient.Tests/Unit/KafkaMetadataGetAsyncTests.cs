using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Connection;
using KafkaClient.Protocol;
using KafkaClient.Tests.Helpers;
using Moq;
using Ninject.MockingKernel.Moq;
using NSubstitute;
using NUnit.Framework;

namespace KafkaClient.Tests.Unit
{
    [Category("Unit")]
    [TestFixture]
    public class KafkaMetadataGetAsyncTests
    {
        private ILog _log;
        private IBrokerRouter _brokerRouter;

        [SetUp]
        public void Setup()
        {
            _log = Substitute.For<ILog>();
            _brokerRouter = Substitute.For<IBrokerRouter>();
            _brokerRouter.Log.ReturnsForAnyArgs(_log);
            _brokerRouter.Configuration.ReturnsForAnyArgs(new CacheConfiguration());
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

            _brokerRouter.Connections.ReturnsForAnyArgs(new List<IConnection> {conn});
            var response = await _brokerRouter.GetMetadataAsync(new []{ "Test"}, CancellationToken.None);

            Received.InOrder(() =>
            {
                conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), It.IsAny<CancellationToken>());
                _log.WarnFormat("Failed metadata request on attempt {0}: Will retry in {1}", Arg.Any<object[]>());
                conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), It.IsAny<CancellationToken>());
            });
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task ShouldRetryWhenReceiveBrokerIdNegativeOne()
        {
            var conn = Substitute.For<IConnection>();

            conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), It.IsAny<CancellationToken>(), Arg.Any<IRequestContext>())
                .Returns(x => CreateMetadataResponse(-1, "123", 1), x => CreateMetadataResponse(ErrorResponseCode.NoError));

            _brokerRouter.Connections.ReturnsForAnyArgs(new List<IConnection> {conn});
            var response = await _brokerRouter.GetMetadataAsync(new []{ "Test"}, CancellationToken.None);

            Received.InOrder(() =>
            {
                conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), It.IsAny<CancellationToken>());
                _log.WarnFormat("Failed metadata request on attempt {0}: Will retry in {1}", Arg.Any<object[]>());
                conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), It.IsAny<CancellationToken>());
            });
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void ShouldReturnWhenNoErrorReceived()
        {
            var conn = Substitute.For<IConnection>();

            conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), It.IsAny<CancellationToken>())
                .Returns(x => CreateMetadataResponse(ErrorResponseCode.NoError));

            _brokerRouter.Connections.ReturnsForAnyArgs(new List<IConnection> {conn});
            var source = new CancellationTokenSource();
            var response = _brokerRouter.GetMetadataAsync(new [] { "Test"}, source.Token);
            source.Cancel();

            conn.Received(1).SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>());
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void ShouldReturnWhenNoErrorReceivedAndTopicsNotSpecified()
        {
            var conn = Substitute.For<IConnection>();

            conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), It.IsAny<CancellationToken>())
                .Returns(x => CreateMetadataResponse(ErrorResponseCode.NoError));

            _brokerRouter.Connections.ReturnsForAnyArgs(new List<IConnection> {conn});
            var source = new CancellationTokenSource();
            var response = _brokerRouter.GetMetadataAsync(new string[] { }, source.Token);
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

            _brokerRouter.Connections.ReturnsForAnyArgs(new List<IConnection> {conn});
            var response = await _brokerRouter.GetMetadataAsync(new [] { "Test"}, CancellationToken.None);
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [TestCase(null)]
        [TestCase("")]
        [ExpectedException(typeof(ConnectionException))]
        public async Task ShouldThrowExceptionWhenHostIsMissing(string host)
        {
            var conn = Substitute.For<IConnection>();

            conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), It.IsAny<CancellationToken>()).Returns(x => CreateMetadataResponse(1, host, 1));

            _brokerRouter.Connections.ReturnsForAnyArgs(new List<IConnection> {conn});
            var response = await _brokerRouter.GetMetadataAsync(new [] { "Test"}, CancellationToken.None);
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [TestCase(0)]
        [TestCase(-1)]
        [ExpectedException(typeof(ConnectionException))]
        public async Task ShouldThrowExceptionWhenPortIsMissing(int port)
        {
            var conn = Substitute.For<IConnection>();

            conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), It.IsAny<CancellationToken>()).Returns(x => CreateMetadataResponse(1, "123", port));

            _brokerRouter.Connections.ReturnsForAnyArgs(new List<IConnection> {conn});
            var response = await _brokerRouter.GetMetadataAsync(new [] { "Test"}, CancellationToken.None);
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