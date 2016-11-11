using System;
using System.Collections.Generic;
using System.Linq;

namespace KafkaClient.Telemetry
{
    public class StatisticsSummary
    {
        public ProduceRequestSummary ProduceRequestSummary { get; private set; }
        public List<NetworkWriteSummary> NetworkWriteSummaries { get; }

        public List<ProduceRequestStatistic> ProduceRequestStatistics { get; }
        public List<NetworkWriteStatistic> CompletedNetworkWriteStatistics { get; private set; }
        public List<NetworkWriteStatistic> QueuedNetworkWriteStatistics { get; private set; }
        public Gauges Gauges { get; }

        public StatisticsSummary(List<ProduceRequestStatistic> produceRequestStatistics,
                                 List<NetworkWriteStatistic> queuedWrites,
                                 List<NetworkWriteStatistic> completedWrites,
                                 Gauges gauges)
        {
            ProduceRequestStatistics = produceRequestStatistics;
            QueuedNetworkWriteStatistics = queuedWrites;
            CompletedNetworkWriteStatistics = completedWrites;
            Gauges = gauges;

            if (queuedWrites.Count > 0 || completedWrites.Count > 0)
            {
                var queuedSummary = queuedWrites.GroupBy(x => x.Endpoint)
                                                .Select(e => new
                                                {
                                                    Endpoint = e.Key,
                                                    QueuedSummary = new NetworkQueueSummary
                                                    {
                                                        SampleSize = e.Count(),
                                                        OldestBatchInQueue = e.Max(x => x.TotalDuration),
                                                        BytesQueued = e.Sum(x => x.Payload.Buffer.Length),
                                                        QueuedMessages = e.Sum(x => x.Payload.MessageCount),
                                                        QueuedBatchCount = Gauges.QueuedWriteOperation,
                                                    }
                                                }).ToList();

                var networkWriteSampleTimespan = completedWrites.Count <= 0 ? TimeSpan.FromMilliseconds(0) : DateTime.UtcNow - completedWrites.Min(x => x.CreatedOnUtc);
                var completedSummary = completedWrites.GroupBy(x => x.Endpoint)
                                                      .Select(e =>
                                                              new
                                                              {
                                                                  Endpoint = e.Key,
                                                                  CompletedSummary = new NetworkTcpSummary
                                                                  {
                                                                      MessagesPerSecond = (int)(e.Sum(x => x.Payload.MessageCount) /
                                                                          networkWriteSampleTimespan.TotalSeconds),
                                                                      MessagesLastBatch = e.OrderByDescending(x => x.CompletedOnUtc).Select(x => x.Payload.MessageCount).FirstOrDefault(),
                                                                      MaxMessagesPerSecond = e.Max(x => x.Payload.MessageCount),
                                                                      BytesPerSecond = (int)(e.Sum(x => x.Payload.Buffer.Length) /
                                                                          networkWriteSampleTimespan.TotalSeconds),
                                                                      AverageWriteDuration = TimeSpan.FromMilliseconds(e.Sum(x => x.WriteDuration.TotalMilliseconds) /
                                                                          completedWrites.Count),
                                                                      AverageTotalDuration = TimeSpan.FromMilliseconds(e.Sum(x => x.TotalDuration.TotalMilliseconds) /
                                                                          completedWrites.Count),
                                                                      SampleSize = completedWrites.Count
                                                                  }
                                                              }
                                                      ).ToList();

                NetworkWriteSummaries = new List<NetworkWriteSummary>();
                var endpoints = queuedSummary.Select(x => x.Endpoint).Union(completedWrites.Select(x => x.Endpoint));
                foreach (var endpoint in endpoints)
                {
                    NetworkWriteSummaries.Add(new NetworkWriteSummary
                    {
                        Endpoint = endpoint,
                        QueueSummary = queuedSummary.Where(x => x.Endpoint.Equals(endpoint)).Select(x => x.QueuedSummary).FirstOrDefault(),
                        TcpSummary = completedSummary.Where(x => x.Endpoint.Equals(endpoint)).Select(x => x.CompletedSummary).FirstOrDefault()
                    });
                }
            }
            else
            {
                NetworkWriteSummaries = new List<NetworkWriteSummary>();
            }

            if (ProduceRequestStatistics.Count > 0)
            {
                var produceRequestSampleTimespan = DateTime.UtcNow -
                    ProduceRequestStatistics.Min(x => x.CreatedOnUtc);

                ProduceRequestSummary = new ProduceRequestSummary
                {
                    SampleSize = ProduceRequestStatistics.Count,
                    MessageCount = ProduceRequestStatistics.Sum(s => s.MessageCount),
                    MessageBytesPerSecond = (int)
                        (ProduceRequestStatistics.Sum(s => s.MessageBytes) / produceRequestSampleTimespan.TotalSeconds),
                    PayloadBytesPerSecond = (int)
                        (ProduceRequestStatistics.Sum(s => s.PayloadBytes) / produceRequestSampleTimespan.TotalSeconds),
                    CompressedBytesPerSecond = (int)
                        (ProduceRequestStatistics.Sum(s => s.CompressedBytes) / produceRequestSampleTimespan.TotalSeconds),
                    AverageCompressionRatio =
                        Math.Round(ProduceRequestStatistics.Sum(s => s.CompressionRatio) / ProduceRequestStatistics.Count, 4),
                    MessagesPerSecond = (int)
                        (ProduceRequestStatistics.Sum(x => x.MessageCount) / produceRequestSampleTimespan.TotalSeconds)
                };
            }
            else
            {
                ProduceRequestSummary = new ProduceRequestSummary();
            }
        }
    }
}