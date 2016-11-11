using System;

namespace KafkaClient.Telemetry
{
    public static class MathHelper
    {
        public static double ConvertToMegabytes(int bytes)
        {
            if (bytes == 0) return 0;
            return Math.Round((double)bytes / 1048576, 4);
        }

        public static double ConvertToKilobytes(int bytes)
        {
            if (bytes == 0) return 0;
            return Math.Round((double)bytes / 1000, 4);
        }
    }
}