using System;
using System.Collections.Immutable;
using System.Security.Cryptography;
using BenchmarkDotNet.Running;
using KafkaClient.Common;

namespace KafkaClient.Performance
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var summary = BenchmarkRunner.Run<ProduceBenchmark>();
        }
    }
}
