using System.Linq;
using System.Threading.Tasks;
using KafkaClient.Tests.Helpers;
using NUnit.Framework;

namespace KafkaClient.Tests.Unit
{
    [TestFixture]
    [Category("Unit")]
    public class CircularBufferTests
    {
        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public void BufferShouldOnlyStoreMaxAmount()
        {
            var buffer = new StatisticsTracker.ConcurrentCircularBuffer<int>(2);

            for (int i = 0; i < 10; i++)
            {
                buffer.Enqueue(i);
            }

            Assert.That(buffer.Count, Is.EqualTo(2));
        }

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public void BufferShouldCountUntilMaxHitThenAlswaysShowMax()
        {
            var buffer = new StatisticsTracker.ConcurrentCircularBuffer<int>(2);

            Assert.That(buffer.Count, Is.EqualTo(0));
            buffer.Enqueue(1);
            Assert.That(buffer.Count, Is.EqualTo(1));
            buffer.Enqueue(1);
            Assert.That(buffer.Count, Is.EqualTo(2));
            buffer.Enqueue(1);
            Assert.That(buffer.Count, Is.EqualTo(2));
        }

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public void BufferMaxSizeShouldReportMax()
        {
            var buffer = new StatisticsTracker.ConcurrentCircularBuffer<int>(2);

            Assert.That(buffer.MaxSize, Is.EqualTo(2));
            buffer.Enqueue(1);
            Assert.That(buffer.MaxSize, Is.EqualTo(2));
        }

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public void EnumerationShouldReturnOnlyRecordsWithData()
        {
            var buffer = new StatisticsTracker.ConcurrentCircularBuffer<int>(2);
            Assert.That(buffer.ToList().Count, Is.EqualTo(0));

            buffer.Enqueue(1);
            Assert.That(buffer.ToList().Count, Is.EqualTo(1));

            buffer.Enqueue(1);
            buffer.Enqueue(1);
            Assert.That(buffer.ToList().Count, Is.EqualTo(2));
        }

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public void EnqueueShouldAddToFirstSlot()
        {
            var buffer = new StatisticsTracker.ConcurrentCircularBuffer<int>(2);
            buffer.Enqueue(1);
            Assert.That(buffer.First(), Is.EqualTo(1));
        }

        [Test, Repeat(IntegrationConfig.TestAttempts)]
        public void EnqueueCanBeUseFromDefferantThread()
        {
            var buffer = new StatisticsTracker.ConcurrentCircularBuffer<int>(2);

            Parallel.For(0, 1000, (i) =>
            {
                buffer.Enqueue(i);
                Assert.That(buffer.Count, Is.LessThanOrEqualTo(2));
            });
            Assert.That(buffer.Count, Is.EqualTo(2));
        }
    }
}