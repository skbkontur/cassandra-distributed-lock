using System;

using GroboContainer.Infection;

using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Tests.RemoteLockTests
{
    [IgnoredImplementation]
    public class StochasticTimestampProvider : ITimestampProvider
    {
        public StochasticTimestampProvider(TimestampProviderStochasticType stochasticType)
        {
            this.stochasticType = stochasticType;
        }

        public long GetNowTicks()
        {
            var diff = TimeSpan.FromSeconds(Rng.Next(50, 100)).Ticks;
            switch(stochasticType)
            {
            case TimestampProviderStochasticType.None:
                diff = 0;
                break;
            case TimestampProviderStochasticType.OnlyPositive:
                break;
            case TimestampProviderStochasticType.BothPositiveAndNegative:
                diff *= Rng.Next(-1, 2);
                break;
            }
            return DateTime.UtcNow.Ticks + diff;
        }

        private static Random Rng { get { return random ?? (random = new Random(Guid.NewGuid().GetHashCode())); } }

        [ThreadStatic]
        private static Random random;

        private readonly TimestampProviderStochasticType stochasticType;
    }
}