using System;
using System.Runtime.CompilerServices;

namespace KafkaClient.Tests.Helpers
{
    public static class UnitConfig
    {
        public static Uri ServerUri([CallerMemberName] string name = null)
        {
            return new Uri($"http://localhost:{ServerPort(name)}");
        }

        public static int ServerPort([CallerMemberName] string name = null)
        {
            return 10000 + (name ?? "").GetHashCode() % 1000;
        }
    }
}