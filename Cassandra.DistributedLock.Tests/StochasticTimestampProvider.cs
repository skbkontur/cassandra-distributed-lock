using System;

using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;

namespace Cassandra.DistributedLock.Tests
{
    public class StochasticTimestampProvider : ITimestampProvider
    {
        public StochasticTimestampProvider(TimestampProviderStochasticType stochasticType, TimeSpan lockTtl)
        {
            this.stochasticType = stochasticType;
            this.lockTtl = lockTtl;
        }

        public long GetNowTicks()
        {
            // lock algorithm is correct only when time is out of sync by no more than lockTtl.
            long diff = 0;
            switch (stochasticType)
            {
            case TimestampProviderStochasticType.None:
                diff = 0;
                break;
            case TimestampProviderStochasticType.OnlyPositive:
                diff = TimeSpan.FromMilliseconds(Rng.Next(Math.Max(1, (int)lockTtl.TotalMilliseconds / 2), (int)lockTtl.TotalMilliseconds)).Ticks;
                break;
            case TimestampProviderStochasticType.BothPositiveAndNegative:
                diff = TimeSpan.FromMilliseconds(Rng.Next(Math.Max(1, (int)lockTtl.TotalMilliseconds / 4), (int)lockTtl.TotalMilliseconds / 2)).Ticks * Rng.Next(-1, 2);
                break;
            }
            return DateTime.UtcNow.Ticks + diff;
        }

        private static Random Rng => random ?? (random = new Random(Guid.NewGuid().GetHashCode()));

        [ThreadStatic]
        private static Random random;

        private readonly TimestampProviderStochasticType stochasticType;
        private readonly TimeSpan lockTtl;
    }
}