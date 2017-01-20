using BenchmarkDotNet.Running;

namespace KafkaClient.Performance
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var switcher = new BenchmarkSwitcher(new[] {
                typeof(ProduceBenchmark),
                typeof(ProduceRequestBenchmark),
                typeof(FetchBenchmark)
            });
            switcher.Run(args);
        }
    }
}
