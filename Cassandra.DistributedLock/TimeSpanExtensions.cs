using System;

namespace SkbKontur.Cassandra.DistributedLock
{
    internal static class TimeSpanExtensions
    {
        public static TimeSpan Multiply(this TimeSpan value, double factor)
        {
            return TimeSpan.FromTicks((long)(value.Ticks * factor));
        }
    }
}